from requests_futures.sessions import FuturesSession
from bson import ObjectId
import pymongo
from config import config
from datetime import date, datetime, timedelta
import requests
import json
import re
from googleapiclient.discovery import build
import google.oauth2.credentials
import boto3
import os
import time

client = boto3.client('lambda')
sessionRequest = FuturesSession()
session = FuturesSession()
dbConnection = pymongo.MongoClient(os.environ.get('MONGO_URI'))
db = dbConnection["chatbotbuilderV2"]

#start of defining default values used in responses
Default_url = "https://botmywork.com/"
Default_imgurl = "chatbot-img.jpg"
Default_title = "Product Title"
Default_payload = "-1"
Default_admin_response = "No Response From Admin"
Default_template_not_found = "Sorry I couldn't understand. Please try again later. No template found."
Default_not_understand = "Sorry, sometimes it's difficult to understand humans. Our team will contact you very soon."
Default_bmw_response = "Hello, I am Botmywork Chatbot Builder, If you are admin of this page please set your responses on *Get Started* Button. To set the responses please visit: https://botmywork.com/apps/chatbot/conversation."
DefaultNumber = "+919876543210"
DefaultUnsubscribeMessage = "Unsubscribe"
DefaultUnsubscribeOutputMessage = "Unsubscribed Successfully"
DefaultFirstMessage = "This is an automated message powered by BotMyWork Chatbot Builder. Get a Bot for your own Business."
#end of defining default values used in responses

def handleQueueMessages(event, context=None):
    """@handleQueueMessages is used to Handle All the Messages under the Message Queue."""
    queueResponse = True
    for index,record in enumerate(event['Records']):
        record = json.loads(record['body'],strict=False)
        if record and record is not None:
            if record['argsData']['isNewUser'] and not ( record['argsData']['pageId'] == os.environ.get('TESTINGPAGE') or record['argsData']['pageId'] == os.environ.get('CBBMAINPAGE')):
                sendDefaultBrandingMessage(record['argsData'])
                record['argsData']['isNewUser'] = False
            try:
                record['argsData']['isNewUser'] = False
                if record['messageType'] == "template":
                    sendTemplateResponses(record['argsData'], record['blockData'], record['inputText'])
                else:
                    sendTextResponses(record['argsData'], record['blockData']['message'], record['inputText'], record['blockData']['responseType'], record['saveMsg'])
            except Exception as e:
                print('method: handleQueueMessages')
                print(record)
                print(e)
                pass
            # STOPPING TO CREATE LABELS FOR DEFAULT DETAILS
            # generateGuestUserLabels(record['argsData'])
            queueResponse = "Queue of {} is Processed".format(record['argsData']['sender_id'])
    return queueResponse

def generateGuestUserLabels(accessibleData):
    if accessibleData and accessibleData is not None:
        guestuser = fetchGuestUserById({'pageId':accessibleData['pageId'], 'userFbId':accessibleData['sender_id']})
        if guestuser and guestuser is not None:
            if 'details' in guestuser:
                if 'labels' in guestuser:
                    if 'systemLabels' in guestuser['labels']:
                        if not len(guestuser['labels']['systemLabels']) or len(guestuser['labels']['systemLabels']) == 0:
                            allLabels = assignLabelsNewUsers(guestuser['details'], accessibleData)
                            db.guestUsers.update_one(
                                { 'userFbId': accessibleData['sender_id'], 'pageId':accessibleData['pageId'] },
                                { '$set': { 'labels.systemLabels': allLabels['systemLabels'] }},
                                upsert=True
                            )
                else:
                    allLabels = assignLabelsNewUsers(guestuser['details'], accessibleData)
                    db.guestUsers.update_one(
                        { 'userFbId': accessibleData['sender_id'], 'pageId':accessibleData['pageId'] },
                        { '$set': { 'labels.systemLabels': allLabels['systemLabels'] }},
                        upsert=True
                    )
    return

def fetchGuestUserById(params, projections = None):
    """@fetchGuestUserById returns the Guest User via pageId"""
    GuestUserData = None
    try:
        GuestUserData = db.guestUsers.find_one(params, projections)
    except:
        GuestUserData = None
    return GuestUserData

def sendDefaultBrandingMessage(kwargs):
    """@sendDefaultBrandingMessage is used to send first message as branding for free users."""
    if not checkProUser(kwargs):
        postData = {
            'message': {
                "attachment":{
                    "type":"template",
                    "payload":{
                        "template_type":"button",
                        "text": DefaultFirstMessage,
                        "buttons": [{
                            'type': 'web_url',
                            'url': config['CBBLANDINGPAGE'],
                            'title': "Get it now \u26a1"
                        }]
                    }
                }
            }, 
            'recipient': {}
        } 
        try:
            response = send_response(kwargs, postData)
            if response.status_code == 200:
                savepostData = {
                    'message': postData['message'],
                    'type': "button"
                }
                if not kwargs.get('isOptin', False):
                    db.guestUsers.update_one(
                        { 'userFbId': kwargs['sender_id'], 'pageId':kwargs['pageId'] },
                        { '$set': {'lastMessage' : savepostData, 'defaultSent': kwargs['defaultSent'] } },
                        upsert=True
                    )
        except Exception as e:
            print('method: sendDefaultBrandingMessage')
            print(kwargs)
            print(str(e))

def checkProUser(kwargs):
    """@checkProUser is used to check if the Bot used is Pro or not."""
    isPro = False
    if 'paymentPlan' in kwargs:
        if kwargs['paymentPlan']:
            if 'validity' in kwargs['paymentPlan'] and 'isPaid' in kwargs['paymentPlan']:
                if kwargs['paymentPlan']['isPaid']:
                    if kwargs['paymentPlan']['validity'] >= str(datetime.now()):
                        isPro = True
    return isPro

def sendTemplateResponses(kwargs, blockDataBlocks, inputText):
    """@sendTemplateResponses is used to send templates messages"""
    blockCount = 0
    stop = False
    timeData = 0
    blockData = []
    totalBlocks = len(blockDataBlocks)
    storeLastMessage = {}
    storeType = {}
    updateDataInDb = {}
    if not totalBlocks and not kwargs['isOptin']:
        db.guestUsers.update_one(
            { 'userFbId': kwargs['sender_id'], 'pageId':kwargs['pageId'] },
            { '$set': {'lastMessage' : {}, 'defaultSent': kwargs['defaultSent'], 'blockDataBlocks': blockData } },
            upsert=True)
    else:
        for block in blockDataBlocks:
            isCardExecuted = False
            blockCount = blockCount + 1
            validCard = False 
            if 'meta' in block:
                if 'valid' in block['meta']:
                    validCard = block['meta']['valid']
            elif 'isInvalid' in block:
                validCard = True if not block['isInvalid'] else False
            execute = True
            if stop or blockCount > 50:
                break
            postData = {'message':{}, 'recipient':{}}
            savepostData = {'message':{}}
            saveReData = {}
            current = {}
            block_type = block['template_type']
            if block_type == 'quick_reply':
                postData['message'], current['type'], setAttribute  = quickReplies(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
                if setAttribute and setAttribute is not None:
                    savepostData['attribute'] = setAttribute
                if storeLastMessage and storeLastMessage is not None:
                    if storeType['type'] in 'text':
                        postData['message']['text'] = storeLastMessage['text']
                    else:
                        postData['message']['attachment'] = storeLastMessage['attachment']
                else:
                    updateDataInDb.update({'lastMessage':{},'defaultSent': kwargs['defaultSent'], 'blockDataBlocks': blockData })
                    db.guestUsers.update_one(
                        { 'userFbId': kwargs['sender_id'], 'pageId':kwargs['pageId'] },
                        { '$set': updateDataInDb }
                    )
                    continue
            elif block_type == 'buttons':
                postData['message'], current['type'] = buttonTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
            elif block_type == 'input':
                postData['message'], current['type'], saveReData = inputTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': saveReData,
                    'type': current['type']
                }
                blockData = blockDataBlocks[blockCount::]
                stop = True
            elif block_type == 'user_input':
                postData['message'], current['type'], saveReData = userInputTemplate(kwargs, block, inputText)
                savepostData = {
                    'message':saveReData,
                    'type': current['type'],
                    'count': 0
                }
                if 'message' in savepostData:
                    if 'user_input_type' in savepostData['message']:
                        if savepostData['message']['user_input_type'] == 'counter':
                            savepostData['index'] = 0
                blockData = blockDataBlocks[blockCount::]
                stop = True
            elif block_type == 'generic':
                postData['message'], current['type'] = genericTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
            elif block_type == 'list':
                postData['message'], current['type'] = listTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
            elif block_type == 'json':         
                isBreak = webhookTemplate(kwargs, block, inputText)
                if isBreak:
                    if 'id' in block and 'meta' in block:
                        db.userBlocks.find_one_and_update({'botId': kwargs['botId'],'blockData.blocks': {"$elemMatch":{ "id":block['id']}} },
                        { "$inc": { 'blockData.blocks.$.meta.impressions': 1} })
                    break
                isCardExecuted = True
            elif block_type == 'google_sheet_integration':          
                block = googleSheetIntegrationTemplate(kwargs, block, inputText)
            elif block_type == 'google_sheet_integration_v2':          
                block = googleSheetIntegration_V2_Template(kwargs, block, inputText)
            elif block_type == 'notify_admin_email' and validCard:
                notifyToAdminViaEmailTemplate(kwargs, block, inputText)
                isCardExecuted = True
            elif block_type == 'export-via-zapier' and validCard:
                pluginName = block['title'] + " "+ block['id']
                notifyToZapier({ "type": "export_zapier", "data" : { "botId": kwargs['botId'], "subscriberId": kwargs['guestUserId'], "pageId": kwargs['pageId'], 'pluginName': pluginName }})
                isCardExecuted = True
            ## clear validation card ####
            elif block_type == 'clear_Validation' and validCard:
                isCardExecuted = clearValidationTemplate(kwargs, block, inputText)
            ####end of clear validation card ######
            ## proto type for sequence ######
            elif block_type == 'subscribe_sequence' and validCard:
                isCardExecuted = subsribeSequenceTemplate(kwargs, block, inputText)
            elif block_type == 'unsubscribe_sequence' and validCard:
                isCardExecuted = unsubsribeSequenceTemplate(kwargs, block, inputText)
            ## end of proto type ####
            elif block_type == 'takeover_chat' and validCard:
                postData['message'], current['type'], notificationToSend = takeoverChatTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
                if notificationToSend:
                    userInDb = db.users.find_one({'_id': ObjectId(notificationToSend['admin_id'])})
                    if userInDb and userInDb is not None:
                        if 'fbid' in userInDb:
                            guestUsersAdminInDb = db.guestUsers.find_one({'pageId':kwargs['pageId'], 'asid': userInDb['fbid']},{'userFbId': 1})
                            if guestUsersAdminInDb and guestUsersAdminInDb is not None:
                                if 'userFbId' in guestUsersAdminInDb:
                                    post_data = {'message':{}, 'recipient':{}}
                                    post_data['message']['text'] = notificationToSend['message']
                                    adminInfo = { 
                                        'sender_id': guestUsersAdminInDb['userFbId'],
                                        'token': kwargs['token']
                                    }
                                    send_response(adminInfo, post_data)
                                    saveData = {
                                        'message': post_data['message'],
                                        'type': 'text'
                                    }
                                    db.guestUsers.update_one({'_id': guestUsersAdminInDb['_id']},{'$set': {'lastMessage' : saveData}})                   
                if postData['message'] and postData['message'] is not None and execute:
                    if 'time' in block['duration'] and 'type' in block['duration']:
                        duration = block['duration']['time'] * 60 if block['duration']['type'] == 'hour' else block['duration']['time']
                    else:
                        duration = 30
                    updateDataInDb['automation'] = {
                        'isDisabled': True,
                        'time': int(datetime.now().strftime("%s")) + duration * 60,
                        'type':'auto'
                    }
                    blockData = blockDataBlocks[blockCount::]
                    stop = True
            # elif block_type == 'products':
            #     postData['message'], current['type'] = productTemplate(kwargs, block, inputText)
            #     savepostData = {
            #         'message': postData['message'],
            #         'type': current['type']
            #     }
            elif block_type == 'media':
                postData['message'], current['type'] = mediaTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
            elif block_type == 'video' and validCard:
                postData['message'], current['type'] = videoTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
            elif block_type == 'audio':
                postData['message'], current['type'] = audioTemplate(kwargs, block, inputText)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
            elif block_type == 'typing' and  totalBlocks > 1 and totalBlocks != blockCount :   
                if 'time' in block:
                    if block['time'] and block['time'] is not None:
                        if block['time'] < 10 and block['time'] > 0:
                            timeData = timeData + block['time']
                            start = {'sender_action':'typing_on','recipient':{}}
                            current['type'] = 'typing'
                            if kwargs['isOptin']:
                                sendStatus = send_responseForCheckbox(kwargs, start)
                            else:
                                sendStatus = send_response(kwargs, start)
                            if timeData < 10:
                                time.sleep(block['time'])
            elif block_type == 'attribute' and validCard:
                isCardExecuted = assignAttributeTemplate(kwargs,block,inputText)
            elif block_type == 'redirectTo':
                isCardExecuted = initiateRedirectTemplate(kwargs,block,inputText)
                if isCardExecuted:
                    if 'id' in block and 'meta' in block:
                        db.userBlocks.find_one_and_update({'botId': kwargs['botId'],'blockData.blocks': {"$elemMatch":{ "id":block['id']}} },
                        { "$inc": { 'blockData.blocks.$.meta.impressions': 1} })
                    break
            elif block_type == 'redirect':
                isCardExecuted = handleRedirectCard(kwargs,block,inputText)
                if isCardExecuted:
                    if 'id' in block and 'meta' in block:
                        db.userBlocks.update_one({'botId': kwargs['botId'],'blockData.blocks': {"$elemMatch":{ "id":block['id']}} },
                        { "$inc": { 'blockData.blocks.$.meta.impressions': 1} })
                    break
            elif block_type == 'chat_handover':
                isCardExecuted = passThreadControl(kwargs, block)
                # if isCardExecuted.status_code == 200:
                blockData = blockDataBlocks[blockCount::]
                stop=True
                setHandoverData(kwargs, block, blockData)
            elif block_type == 'active-campaign':
                isCardExecuted = handleActiveCampaign(kwargs, block, inputText)
            elif block_type == 'ulm':
                isCardExecuted = handleUserLevelPeristMenu(kwargs, block)
            elif block_type == 'otn_request':
                postData['message'], current['type'] = sendOtnRequest(kwargs, block)
                savepostData = {
                    'message': postData['message'],
                    'type': current['type']
                }
            if blockCount < totalBlocks:
                if blockDataBlocks[blockCount]['template_type'] == 'quick_reply' and current.get('type') in ['text', 'generic', 'media']:
                    storeLastMessage = postData['message']
                    storeType['type'] = current['type']
                    execute = False
            if postData['message'] and postData['message'] is not None and execute:
                if current['type'] == 'quickReply' or current['type'] == 'input':
                    stop = True
                if kwargs['isOptin']:
                    sendStatus = send_responseForCheckbox(kwargs, postData)
                else:
                    sendStatus = send_response(kwargs, postData)
                if sendStatus.status_code == 200:
                    isCardExecuted = True
                if sendStatus.status_code != 200 and blockDataBlocks[blockCount-2]['template_type'] == 'typing':
                    print(sendStatus)
                    end = {'sender_action':'typing_off','recipient':{}}
                    if kwargs['isOptin']:
                        send_responseForCheckbox(kwargs, end)
                    else:
                        send_response(kwargs, end)
                if not kwargs['isOptin']:
                    updateDataInDb.update({'lastMessage' : savepostData, 'defaultSent': kwargs['defaultSent'], 'blockDataBlocks': blockData })
                    db.guestUsers.update_one(
                        { 'userFbId': kwargs['sender_id'], 'pageId':kwargs['pageId'] },
                        { '$set': updateDataInDb },
                        upsert=True
                    )
                else:
                    updateDataInDb.update({'lastMessage' : savepostData, 'defaultSent': kwargs['defaultSent'], 'blockDataBlocks': blockData })
                    db.optinSubscribers.update_one(
                        { 'user_ref': kwargs['sender_id'], 'pageId':kwargs['pageId'] },
                        { '$set': updateDataInDb },
                        upsert=True
                    )
            elif current.get('type') == 'typing' and not kwargs['isOptin']:
                updateDataInDb.update({'lastMessage' : savepostData, 'defaultSent': kwargs['defaultSent'], 'blockDataBlocks': blockData })
                db.guestUsers.update_one(
                    { 'userFbId': kwargs['sender_id'], 'pageId':kwargs['pageId'] },
                    { '$set': updateDataInDb },
                    upsert=True
                )
            if isCardExecuted:
                if 'id' in block and 'meta' in block:
                    db.userBlocks.find_one_and_update({'botId': kwargs['botId'],'blockData.blocks': {"$elemMatch":{ "id":block['id']}} },
                    { "$inc": { 'blockData.blocks.$.meta.impressions': 1} })
            # if block_type == 'typing' and  totalBlocks > 1 and totalBlocks != blockCount:
            #     time.sleep(timeData-1)
                # queueToInsert = {
                #     'argsData':kwargs,
                #     'blockData':blockDataBlocks[blockCount::],
                #     'inputText':inputText,
                #     'messageType':'template',
                #     'queueTime':str(datetime.now())
                # }
                # sendToMessageTypingQueue(queueToInsert,timeData)
                # break
    return

def setHandoverData(kwargs, block, remainingBlocks=[]):
    """@setHandoverData is used to set handover info and remaining cards of guestUser"""
    setData = {'handover.appId': block['secondary_app_id'],
                                   'handover.lastChanged': int(datetime.now().strftime("%s")),
                                   'handover.onStandby': True, 'handover.lastEventType': 'BMW_CUSTOM_card_pass_control',
                                   'lastUpdated': int(datetime.now().strftime("%s"))*1000,
                                   'handover.timeout': int(datetime.now().strftime("%s")),
                                   'defaultSent': kwargs['defaultSent'], 'blockDataBlocks': remainingBlocks,
                                   'onChat': True }
    if 'timeout' in block:
        if 'value' in block['timeout'] and 'unit' in block['timeout']:
            if (isinstance(block['timeout']['value'],int) or block['timeout']['value'].isdigit()) and block['timeout']['unit']:
                val = int(block['timeout']['value'])
                if block['timeout']['unit'] == 'seconds':
                    setData['handover.timeout'] = int((datetime.now() + timedelta(seconds=val)).strftime("%s"))
                elif block['timeout']['unit'] == 'minutes':
                    setData['handover.timeout'] = int((datetime.now() + timedelta(minutes=val)).strftime("%s"))
                elif block['timeout']['unit'] == 'hours':
                    setData['handover.timeout'] = int((datetime.now() + timedelta(hours=val)).strftime("%s"))
                elif block['timeout']['unit'] == 'days':
                    setData['handover.timeout'] = int((datetime.now() + timedelta(days=val)).strftime("%s"))
                elif block['timeout']['unit'] == 'off':
                    setData['handover.timeout'] = int((datetime.now() + timedelta(days=366)).strftime('%s'))
    setData['handover.timeout'] = setData['handover.timeout']*1000
    if 'pass_phrase' in block:
        if isinstance(block['pass_phrase'], list):
            setData['handover.pass_phrase'] = block['pass_phrase']
    db.guestUsers.update_one({'userFbId': kwargs['sender_id'], 'pageId': kwargs['pageId']},
                            {'$set':setData})

def createPayload(prefix, payload):
    """@createPayload is used to create a unique
    payload with prefix as needed."""
    if isValidObjectId(payload):
        payload = prefix + ":" + payload
    else:
        payload = config['CBBACTIONPAYLOADPREFIX'] + ":" + payload
    return payload

def getImageUrl(filename):
    """@getImageUrl returns the attachments url"""
    return config['ADMINACCESSURL'] + config['UPLOAD_FOLDER'] + '/' + filename

def generateAttachmentId(pageToken, attachmentUrl, attachmentType):
    """@generateAttachmentId is used to get the attachment id by uploading the file to facebook."""
    attachmentId = False
    try:
        fileData = {
            "message": {
                "attachment": {
                    "type":attachmentType.lower(),
                    "payload":{
                    "is_reusable": False,
                    "url":attachmentUrl
                  }
                }
            }
        }
        r = requests.post("https://graph.facebook.com/v12.0/me/message_attachments",
            params={'access_token': str(pageToken)},
            data = json.dumps(fileData),
            headers={'Content-Type': 'application/json'})
        if r.status_code == 200:
            responseData = json.loads(r.text,strict=False)
            attachmentId = responseData['attachment_id']
    except:
        attachmentId = False
    return attachmentId

def isValidObjectId(id):
    """@isValidObjectId is used to validate ObjectId"""
    try:
        ObjectId(id)
        return True
    except:
        return False

def sendTextResponses(kwargs, message, inputText, responseType, saveMsg):
    """@sendTextResponses is used to send Text messages when 
    the queue is running and having type text."""
    postData = {'message':{}, 'recipient':{}}
    if "{{" in message and "}}" in message:
        postData['message']['text'] = fetchUserDeatilsViaCode(kwargs, message, inputText)
    else:
        postData['message']['text'] = message
    savepostData = {
        'message': postData['message']['text'],
        'type': responseType
    }
    send_response(kwargs, postData)
    if saveMsg:
        db.guestUsers.update_one(
            { 'userFbId': kwargs['sender_id'], 'pageId': kwargs['pageId'] },
            { '$set': {'lastMessage' : savepostData, 'defaultSent':kwargs['defaultSent']} },
            upsert=True
        )
    return

def assignLabelsNewUsers(responseData, accessibleData):
    """@assignLabelsNewUsers is used to assign 
    labels to the new Guest users."""
    labels = []
    returnData = {"systemLabels":[]}
    if responseData and responseData is not None:
        if 'name' in responseData:
            if responseData['name'] != "Anonymous User":
                labelData = createLabelDynamically("name", responseData['name'], accessibleData)
                labels.append(labelData)
        if 'first_name' in responseData:
            labelData = createLabelDynamically("first_name", responseData['first_name'], accessibleData)
            labels.append(labelData)
        if 'last_name' in responseData:
            labelData = createLabelDynamically("last_name", responseData['last_name'], accessibleData)
            labels.append(labelData)
        if 'gender' in responseData:
            labelData = createLabelDynamically("gender", responseData['gender'], accessibleData)
            labels.append(labelData)
        if 'locale' in responseData:
            labelData = createLabelDynamically("locale", responseData['locale'], accessibleData)
            labels.append(labelData)
        if 'timezone' in responseData:
            labelData = createLabelDynamically("timezone", responseData['timezone'], accessibleData)
            labels.append(labelData)
        returnData['systemLabels'] = labels
        return returnData

def createLabelDynamically(labelKey, labelName, accessibleData):
    """@createLabelDynamically is used to create custom
    labels dynamically if not exists."""
    labelInDb = checkLabelExists(labelKey, labelName, accessibleData)
    if labelInDb is None:
        tag = cbbLabelCreation(labelKey, labelName, accessibleData)
        if tag and tag is not None:
            db.pagesMeta.update_one({'pageId':accessibleData['pageId']},
                { "$addToSet":{"tags":tag}}, upsert=True)
    else:
        tag = labelInDb['tags'][0]
    return tag

def cbbLabelCreation(labelKey, labelName, accessibleData):
    """@cbbLabelCreation is used to create new label"""
    baseData = str(config['SYSTEMPREFIX']) + str(labelKey) + "$" + str(labelName)
    tag = {
        "labelName":{
            "key":labelKey,
            "value":labelName,
            "base":baseData
            },
        "labelId":str(ObjectId())
        }
    return tag

def checkLabelExists(labelKey, labelName, accessibleData):
    """@checkLabelExists used to check
    label exists in db or not"""
    baseData = str(config['SYSTEMPREFIX']) + str(labelKey) + "$" + str(labelName)
    labelInDb = db.pagesMeta.find_one(
        {    'pageId': accessibleData['pageId'],
            "tags":{"$elemMatch":{ "labelName.key":labelKey,"labelName.base":baseData}}
        },
        {
            "_id":0,
            "tags":{"$elemMatch":{"labelName.base":baseData}}
        })
    return labelInDb

def quickReplies(kwargs, template_data, inputText):
    """@quickReplies is used to build json of Quick Reply 
    type and called from @fetchTemplateData.
    """
    postBack = {}
    quick_replies = []
    setAttribute = None
    quickReply = template_data
    returnType = 'quickReply'
    if 'attribute' in template_data:
        if template_data['attribute'] and template_data['attribute'] is not None:
            attribute = template_data['attribute'].strip()
            if attribute and attribute is not None:
                setAttribute = attribute
    if 'buttons' in quickReply:
        for data in quickReply['buttons']:
            if 'type' in data:
                if data['type'] == 'postback' and 'payload' in data and 'title' in data:
                    productTitle = data['title'] if data['title'] and data['title'] is not None else Default_title
                    payload1 = data['payload'] if data['payload'] and data['payload'] is not None else Default_payload
                    quick_replies.append({
                        'content_type': 'text',
                        'title': productTitle,
                        'payload': createPayload(config['CBBQUICKREPLYPAYLOADPREFIX'], payload1)
                    })
        if quick_replies and quick_replies is not None:
            postBack['quick_replies'] = quick_replies
    return postBack ,returnType, setAttribute

def sendOtnRequest(kwargs, block):
    """@sendOtnRequest is used to build json of one_time_notif_req
     and called from @fetchTemplateData.
    """
    postBack = None
    if 'title' in block and 'payload' in block and 'otn_topic' in block:
        if block['title'] is not None and block['payload'] is not None:
            if isValidObjectId(block['payload']) and isValidObjectId(block['otn_topic']):
                if "{{" in block['title'] and "}}" in block['title']:
                    block['title'] = fetchUserDeatilsViaCode(kwargs, block['title'])
                postBack = {"attachment": {
                        "type":"template",
                        "payload": {
                            "template_type":"one_time_notif_req",
                            "title":block['title'],
                            "payload":block['otn_topic'] + '_cbb_' + block['payload']
                        }
                    }
                }
    return postBack, 'otn_request'

def buttonTemplate(kwargs,  template_data, inputText):
    """@buttonTemplate is used to build json of Button 
    type and called from @fetchTemplateData.
    """
    postBack = {}
    buttons = []
    returnType = 'button'
    defaultRatio = "tall"
    if 'buttons' in template_data:
        for data in template_data['buttons']:
            butarr = {}
            if 'type' in data and 'title' in data:
                if data['type'] == 'web_url':
                    button_url = Default_url
                    if 'url' in data:
                        button_url = data['url'] if data['url'] and data['url'] is not None else button_url
                    button_title = data['title'] if data['title'] and data['title'] is not None else Default_title
                    if "{{" in button_url and "}}" in button_url:
                        button_url = fetchUserDeatilsViaCode(kwargs, button_url, inputText)
                    else:
                        button_url = button_url
                    butarr = {
                        'type': 'web_url',
                        'url': button_url,
                        'title': button_title
                    }
                    if 'webview_height_ratio' in data:
                        butarr['webview_height_ratio'] = data['webview_height_ratio'] if data['webview_height_ratio'] and data['webview_height_ratio'] is not None else defaultRatio
                    buttons.append(butarr)
                elif data['type'] == 'postback':
                    button_payload = Default_payload
                    if 'payload' in data:
                        button_payload = data['payload'] if data['payload'] and data['payload'] is not None else button_payload
                    button_title = data['title'] if data['title'] and data['title'] is not None else Default_title
                    butarr = {
                        'type': 'postback',
                        'title': button_title,
                        'payload': createPayload(config['CBBBUTTONPAYLOADPREFIX'], button_payload)
                    }
                    buttons.append(butarr)
                elif data['type'] == 'phone_number':
                    button_payload = DefaultNumber
                    if 'phone' in data:
                        button_payload = data['phone'] if data['phone'] and data['phone'] is not None else DefaultNumber
                    button_title = data['title'] if data['title'] and data['title'] is not None else Default_title
                    butarr = {
                        'type': 'phone_number',
                        'title': button_title,
                        'payload': button_payload
                    }
                    buttons.append(butarr)
    if 'text' in template_data:
        template_text = template_data['text'] if template_data['text'] and template_data['text'] is not None else "No Response"
        if "{{" in template_text and "}}" in template_text:
            template_text = fetchUserDeatilsViaCode(kwargs, template_text, inputText)
        else:
            template_text = template_text
        if len(buttons):
            postBack = {
                "attachment":{
                    "type":"template",
                    "payload":{
                        "template_type":"button",
                        "text": template_text,
                        "buttons": buttons
                    }
                }
            }
        else:
            returnType = 'text'
            postBack['text'] = template_text
    return postBack, returnType

def inputTemplate(kwargs, template_data, inputText):
    """@inputTemplate is used to build json of Input Type 
    template(QUICKREPLY) and called from @fetchTemplateData."""
    postBack = {}
    savePostBack = {}
    quick_replies_save = []
    quick_replies = []
    quickReply = template_data
    returnType = 'input'
    if 'attribute' in quickReply and 'text' in quickReply and 'validation' in quickReply and 'type' in quickReply:
        if quickReply['type'] == 'user_email' or quickReply['type'] == 'user_phone_number' and quickReply['validation']:
            quick_replies.append({
                'content_type': quickReply['type']
            })
            objectToSave = {
                'content_type': quickReply['type'],
                'attribute' : quickReply['attribute'],
                'isRequired': False
            }
            if quickReply['validation'] == "phone" or quickReply['validation'] == "email":
                objectToSave['isRequired'] = True
                if quickReply['validation'] == "phone":
                    objectToSave['failedMessage'] = "Your input doesn't belong to Phone Number."
                else:
                    objectToSave['failedMessage'] = "Your input doesn't belong to Email."
                if 'defaultMessage' in quickReply:
                    if isinstance(quickReply['defaultMessage'],str):
                        defaultMessage = quickReply['defaultMessage'].strip()
                        if defaultMessage is not None:
                            objectToSave['failedMessage'] = defaultMessage
            quick_replies_save.append(objectToSave)
        if quick_replies and quick_replies is not None and quickReply['text'] and quickReply['text'] is not None:
            postBack['quick_replies'] = quick_replies
            if "{{" in quickReply['text'] and "}}" in quickReply['text']:
                postBack['text'] = fetchUserDeatilsViaCode(kwargs, quickReply['text'], inputText)
            else:
                postBack['text'] = quickReply['text']
            savePostBack['quick_replies'] = quick_replies_save
            savePostBack['text'] = postBack['text']
    return postBack ,returnType, savePostBack

def videoTemplate(kwargs,  template_data, inputText):
    """@videoTemplate is used to build json of video 
    type and called from @fetchTemplateData.
    """
    postBack = {}        
    templates = []
    returnType = 'video'
    for data in template_data['elements']:
        if 'attachment_id' in data and 'media_type' in data:
            if data['media_type'] == 'video' or (data['attachment_id'] and data['attachment_id'] is not None or data['url'] and data['url'] is not None ):
                if 'attachment_id' in data:
                    url = data['attachment_id'].strip()
                    if url.startswith("https://www.facebook.com/") or url.startswith("https://business.facebook.com/"):
                        inset = {
                            "media_type":data['media_type'],
                            "url": url
                        }
                        templates.append(inset)
                    elif url.startswith("https://"):
                        attachmentId = generateAttachmentId(kwargs['token'], url, data['media_type'])
                        if attachmentId:
                            inset = {
                                "media_type":data['media_type'],
                                "attachment_id":attachmentId
                            }
                            templates.append(inset)
    if len(templates):
        postBack = {
            "attachment": {
                "type":"template",
                "payload": {
                    "template_type":"media",
                    "elements": templates
                }
            }
        }
    return postBack, returnType

def audioTemplate(kwargs,  template_data, inputText):
    """@videoTemplate is used to build json of video 
    type and called from @fetchTemplateData.
    """
    postBack = {}        
    returnType = 'audio'
    audioUrl = None
    if 'elements' in template_data and isinstance(template_data['elements'], list):
        regex = re.compile(
                    r'^(?:https)://' # http:// or https://
                    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                    # r'localhost|' #localhost...
                    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                    r'(?::\d+)?' # optional port
                    r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        for element in template_data['elements']:
            if 'attachment_id' in element:
                if re.match(regex, element['attachment_id']):
                    audioUrl = element['attachment_id']
                    break
    if audioUrl is not None:
        postBack = {
            "attachment": {
                "type":"audio",
                "payload": {
                    "url":audioUrl
                }
            }
        }
        
    return postBack, returnType

def takeoverChatTemplate(kwargs, template_data, inputText):
    """@takeoverChatTemplate is used to build talk to agent feature of 
    called from @fetchTemplateData."""
    postBack = {}
    notification = False
    galleryTitle = "Your chat is now connected with our agent ."
    gallerySubTitle = 'To stop chat with agent click on stop chat button or just send "stop chat" message.'
    galleryButton = 'Stop Chat'
    returnType = 'takeover_chat'
    title = template_data['gallery']['title'] if 'title' in template_data['gallery'] else galleryTitle
    subtitle = template_data['gallery']['subtitle'] if 'subtitle' in template_data['gallery'] else gallerySubTitle
    buttonTitle = template_data['gallery']['buttonTitle'] if 'buttonTitle' in template_data['gallery'] else galleryButton
    image_url = template_data['gallery']['imageUrl'] if 'imageUrl' in template_data['gallery'] else None
    template =[
        {
            "title":title,
            "subtitle":subtitle,
            "buttons":[
            {
                "type":"postback",
                "title":buttonTitle,
                "payload": config['CBBGALLERYPAYLOADPREFIX']+":stop_chat"
            }              
            ]      
        }
    ]
    if image_url and image_url is not None:
        template[0]['image_url'] = getImageUrl(image_url)
    postBack = {
        "attachment": {
            "type":"template",
            "payload": {
                "template_type":"generic",
                "elements": template
            }
        }
    }
    if 'isAllow' in template_data['notifyAdmin'] and 'admin_id' in template_data['notifyAdmin']:
        if template_data['notifyAdmin']['isAllow'] and isValidObjectId(template_data['notifyAdmin']['admin_id']):
            notification = {
                'message':'{{name}} want to start a conversation with you. To answer the user go to '+config['SITEBASEURL']+'/cbb/bot/'+kwargs['botId']+'/inbox/'+kwargs['guestUserId'],
                'admin_id': template_data['notifyAdmin']['admin_id']
            }
            notification['message'] = fetchUserDeatilsViaCode(kwargs, notification['message'], inputText)
    return postBack ,returnType, notification

def userInputTemplate(kwargs, template_data, inputText):
    """@userInputTemplate is used to build json of user Input Type 
    template and called from @fetchTemplateData."""
    postBack = {}
    savePostBack = {}
    returnType = 'input'
    if 'text' in template_data and 'validation' in template_data and 'attribute' in template_data:
        if template_data and template_data is not None and template_data['text'] and template_data['text'] is not None and template_data['validation'] and template_data['validation'] is not None:
            if "{{" in template_data['text'] and "}}" in template_data['text']:
                postBack['text'] = fetchUserDeatilsViaCode(kwargs, template_data['text'], inputText)
            else:
                postBack['text'] = template_data['text']
            savePostBack['text'] = postBack['text']
            savePostBack['attribute'] = template_data['attribute']
            if 'buttons' in template_data and template_data['validation'] == 'multi':
                pass
                # multi section in on hold
            elif template_data['validation'] == 'counter' and 'counter' in template_data:
                savePostBack['user_input_type'] = template_data['validation']
                savePostBack['counter'] = template_data['counter']
            elif template_data['validation'] == 'text':
                savePostBack['user_input_type'] = template_data['validation']
            else:
                savePostBack['user_input_type'] = template_data['validation']
                validateMsg = template_data['validation'] if not template_data['validation'] == "datetime" else "date & time"
                savePostBack['failedMessage'] = "Your input doesn't belongs to "+ validateMsg
                if 'defaultMessage' in template_data:
                    if isinstance(template_data['defaultMessage'],str):
                        if template_data['defaultMessage'].strip() is not None:
                            savePostBack['failedMessage'] = template_data['defaultMessage']
    return postBack ,returnType, savePostBack

def genericTemplate(kwargs,  blockData, inputText):
    """@genericTemplate is used to build json of Generic 
    type and called from @fetchTemplateData.
    """
    postBack = {}        
    templates = []
    defaultRatio = "tall"
    returnType = 'generic'
    image_aspect_ratio = 'horizontal'
    if 'is_aspect_ratio_square' in blockData:
        if blockData['is_aspect_ratio_square']:
            image_aspect_ratio = 'square'
    for data in blockData['elements']:
        buttons = []
        if data['buttons'] and len(data['buttons']) != 0 and data['buttons'] is not None:
            for but in data['buttons']:
                if len(but) !=0 and but is not None:
                    butarr = {}
                    if 'type' in but and 'title' in but:
                        button_title = but['title'] if but['title'] and but['title'] is not None else Default_title
                        if but['type'] == 'web_url':
                            button_url = Default_url
                            if 'url' in but:
                                button_url = but['url'] if but['url'] and but['url'] is not None else Default_url
                            if "{{" in button_url and "}}" in button_url:
                                button_url = fetchUserDeatilsViaCode(kwargs, button_url, inputText)
                            else:
                                button_url = button_url
                            butarr = {
                                "type": 'web_url',
                                "url": button_url,
                                "title": button_title
                            }
                            if 'webview_height_ratio' in but:
                                butarr['webview_height_ratio'] = but['webview_height_ratio'] if but['webview_height_ratio'] and but['webview_height_ratio'] is not None else defaultRatio
                            buttons.append(butarr)
                        elif but['type'] == 'postback':
                            button_payload = Default_payload
                            if 'payload' in but:
                                button_payload = but['payload'] if but['payload'] and but['payload'] is not None else Default_payload
                            butarr = {
                                "type": 'postback',
                                "title": button_title,
                                "payload": createPayload(config['CBBGALLERYPAYLOADPREFIX'], button_payload)
                            }
                            buttons.append(butarr)
                        elif but['type'] == 'phone_number':
                            button_payload = DefaultNumber
                            if 'phone' in but:
                                button_payload = but['phone'] if but['phone'] and but['phone'] is not None else button_payload
                            butarr = {
                                "type": 'phone_number',
                                "title": button_title,
                                "payload": button_payload
                            }
                            buttons.append(butarr)
                        elif but['type'] == 'element_share':
                            butarr = {
                                "type": 'element_share'
                            }
                            buttons.append(butarr)
        default_action_title = data['title'] if data['title'] and data['title'] is not None else Default_title
        default_action_url = data['default_action']['url'] if data['default_action']['url'] and data['default_action']['url'] is not None else Default_url
        inset = {
            "title": default_action_title,
            "default_action": {
                "type": "web_url",
                "url": default_action_url,
                "messenger_extensions": False,
                "webview_height_ratio": "full",
            }
        }
        if data['subtitle'] and data['subtitle'] is not None:
            inset['subtitle'] = data['subtitle']
        if len(buttons):
            inset['buttons'] = buttons
        if 'image_url' in data:
            if data['image_url']:
                inset['image_url'] = getImageUrl(data['image_url'])
        templates.append(inset)
    postBack = {
        "attachment": {
            "type":"template",
            "payload": {
                'image_aspect_ratio': image_aspect_ratio,
                "template_type":"generic",
                "elements": templates
            }
        }
    }
    return postBack, returnType

def listTemplate(kwargs,  blockData, inputText):
    """@listTemplate is used to build json of List 
    type and called from @fetchTemplateData.
    """
    postBack = {}
    elements = []
    buttons = []
    topElement = "large"
    returnType = 'list'
    count = 1
    try:
        if blockData['top_element_style']:
            topElement = "large"
        else:
            topElement = "compact"
    except:
        topElement = "compact"
    defaultRatio = "tall"
    for data in blockData['elements']:
        butarr = {}
        if 'buttons' in data:
            if len(data['buttons']):
                for buttons_data in data['buttons']:
                    if 'type' in buttons_data and 'title' in buttons_data:
                        button_title = buttons_data['title'] if buttons_data['title'] and buttons_data['title'] is not None else Default_title
                        if buttons_data['type'] == 'web_url':
                            button_url = Default_url
                            if 'url' in buttons_data:
                                button_url = buttons_data['url'] if buttons_data['url'] and buttons_data['url'] is not None else Default_url
                            if "{{" in button_url and "}}" in button_url:
                                button_url = fetchUserDeatilsViaCode(kwargs, button_url, inputText)
                            else:
                                button_url = button_url
                            butarr = {
                                'type': 'web_url',
                                'url': button_url,
                                'title': button_title
                            }
                            if 'webview_height_ratio' in buttons_data:
                                butarr['webview_height_ratio'] = buttons_data['webview_height_ratio'] if buttons_data['webview_height_ratio'] and buttons_data['webview_height_ratio'] is not None else defaultRatio
                        elif buttons_data['type'] == 'postback':
                            button_payload = Default_payload
                            if 'payload' in buttons_data:
                                button_payload = buttons_data['payload'] if buttons_data['payload'] and buttons_data['payload'] is not None else Default_payload
                            butarr = {
                                'type': 'postback',
                                'title': button_title,
                                'payload': createPayload(config['CBBLISTPAYLOADPREFIX'], button_payload)
                            }
                        elif buttons_data['type'] == 'phone_number':
                            button_payload = DefaultNumber
                            if 'phone' in buttons_data:
                                button_payload = buttons_data['phone'] if buttons_data['phone'] and buttons_data['phone'] is not None else button_payload
                            butarr = {
                                'type': 'phone_number',
                                'title': button_title,
                                'payload': button_payload
                            }
        default_title = data['title'] if data['title'] and data['title'] is not None else Default_title
        default_action_type = data['default_action']['type'] if data['default_action']['type'] and data['default_action']['type'] is not None else "web_url"
        default_action_url = data['default_action']['url'] if data['default_action']['url'] and data['default_action']['url'] is not None else Default_url
        ndata = {
            'title': default_title,
            'subtitle': data['subtitle'],
            "default_action": {
                "type": default_action_type,
                "url": default_action_url,
                "messenger_extensions": False,
                "webview_height_ratio": "full",
            },
            'buttons': [butarr]
        }
        try:
            if topElement == "large" and count == 1:
                default_image_url = Default_imgurl
                if 'image_url' in data:
                    default_image_url = data['image_url'] if data['image_url'] and data['image_url'] is not None else default_image_url
                ndata['image_url'] = getImageUrl(default_image_url)
            else:
                if 'image_url' in data:
                    if data['image_url']:
                        ndata['image_url'] = getImageUrl(data['image_url'])
        except Exception as e:
            print(e)
        elements.append(ndata)
        count = count + 1
    if 'buttons' in blockData:
        for data1 in blockData['buttons']:
            butarr = {}
            button_title = data1['title'] if data1['title'] and data1['title'] is not None else Default_title
            if data1['type'] == 'web_url':
                button_url = Default_url
                if 'url' in data1:
                    button_url = data1['url'] if data1['url'] and data1['url'] is not None else Default_url
                if "{{" in button_url and "}}" in button_url:
                    button_url = fetchUserDeatilsViaCode(kwargs, button_url, inputText)
                else:
                    button_url = button_url
                butarr = {
                    'type': 'web_url',
                    'url': button_url,
                    'title': button_title
                }
                if 'webview_height_ratio' in data1:
                    butarr['webview_height_ratio'] = data1['webview_height_ratio'] if data1['webview_height_ratio'] and data1['webview_height_ratio'] is not None else defaultRatio
            elif data1['type'] == 'postback':
                button_payload = Default_payload
                if 'payload' in data1:
                    button_payload = data1['payload'] if data1['payload'] and data1['payload'] is not None else Default_payload
                butarr = {
                    'type': 'postback',
                    'title': button_title,
                    'payload': createPayload(config['CBBLISTPAYLOADPREFIX'], button_payload)
                }
            elif data1['type'] == 'phone_number':
                button_payload = DefaultNumber
                if 'phone' in data1:
                    button_payload = data1['phone'] if data1['phone'] and data1['phone'] is not None else button_payload
                butarr = {
                    'type': 'phone_number',
                    'title': button_title,
                    'payload': button_payload
                }
            elif data1['type'] == 'element_share':
                butarr = {
                    'type': 'element_share'
                }
            buttons.append(butarr)
    postBack = {
        "attachment": {
            "type": "template",
            "payload": {
                "template_type": "list",
                "top_element_style": topElement,
                "elements": elements,
                "buttons": buttons
            }
        }
    }

    return postBack, returnType

def googleSheetIntegrationTemplate(kwargs, card, inputText):
    """@googleSheetIntegrationTemplate is used to upate data in sheet by intregation
    """
    if 'template_type' in card and 'account' in card and 'spreadsheet' in card and 'headers' in card:
        if card['template_type'] == 'google_sheet_integration' and isinstance(card['account'], dict):
            try:
                if 'token' in card['account'] and 'refresh_token' in card['account'] and 'id' in card['spreadsheet']:
                    refresh_token = card['account']['refresh_token'].strip()
                    if refresh_token is not None and refresh_token and card['account']['token'].strip() is not None:
                        credentials = google.oauth2.credentials.Credentials(card['account']['token'],
                        refresh_token= refresh_token,
                        token_uri= config['GOOGLE_TOKEN_URI'],
                        client_id= config['GOOGLE_CLIENT_ID'],
                        client_secret= config['GOOGLE_CLIENT_SECRET'])
                        if credentials and credentials.expired and credentials.refresh_token:
                            credentials.refresh(google.auth.transport.requests.Request())
                            card['acoount']['token'] =  credentials.token,
                            card['acoount']['refresh_token'] = credentials.refresh_token
                        service = build('sheets', 'v4', credentials=credentials,cache_discovery=False)
                        header = list(card['headers'])
                        header = createSheetHeaderList(header)
                        row = list()
                        if len(header):
                            row = createObjectToSend(header, kwargs, inputText)
                        row.insert(0,str(datetime.now()))
                        resource = {
                        "majorDimension": "ROWS",
                        "values": [row]
                        }
                        requestData = service.spreadsheets().values().append(spreadsheetId=card['spreadsheet']['id'], range="A2:A4", valueInputOption='USER_ENTERED', body=resource)
                        response = requestData.execute()
                        if 'id' in card and 'meta' in card:
                            db.userBlocks.find_one_and_update({'botId': kwargs['botId'],'blockData.blocks': {"$elemMatch":{ "id":card['id']}} },
                            { "$inc": { 'blockData.blocks.$.meta.impressions': 1} })
            except Exception as e:
                print(str(e))
                updateErrorNotifiation(kwargs,{'title': "Google Sheet card", 'message': 'your sheet entry is not updated'})
    return card


def googleSheetIntegration_V2_Template(kwargs, card, inputText):
    """@googleSheetIntegration_V2_Template is used to upate data in sheet by intregation v2
    """
    if 'template_type' in card and 'account' in card and 'spreadsheet' in card:
        if card['template_type'] == 'google_sheet_integration_v2' and isinstance(card['account'], dict) and 'rowConfig' in card['spreadsheet']:
            try:
                if 'token' in card['account'] and 'refresh_token' in card['account'] and 'id' in card['spreadsheet']:
                    refresh_token = card['account']['refresh_token'].strip()
                    if refresh_token is not None and refresh_token and card['account']['token'].strip() is not None:
                        credentials = google.oauth2.credentials.Credentials(card['account']['token'],
                        refresh_token= refresh_token,
                        token_uri= config['GOOGLE_TOKEN_URI'],
                        client_id= config['GOOGLE_CLIENT_ID'],
                        client_secret= config['GOOGLE_CLIENT_SECRET'])
                        if credentials and credentials.expired and credentials.refresh_token:
                            credentials.refresh(google.auth.transport.requests.Request())
                            card['acoount']['token'] =  credentials.token,
                            card['acoount']['refresh_token'] = credentials.refresh_token
                        service = build('sheets', 'v4', credentials=credentials,cache_discovery=False)
                        rowsConf = list(card['spreadsheet']['rowConfig'])
                        rowValues = [doc.get('value', '') for doc in rowsConf]
                        row = list()
                        if rowValues:
                            row = createObjectToSend_V2(rowValues, kwargs, inputText)
                        resource = {
                        "majorDimension": "ROWS",
                        "values": [row]
                        }
                        requestData = service.spreadsheets().values().append(spreadsheetId=card['spreadsheet']['id'], range="A2:A4", valueInputOption='USER_ENTERED', body=resource)
                        response = requestData.execute()
                        if 'id' in card and 'meta' in card:
                            db.userBlocks.find_one_and_update({'botId': kwargs['botId'],'blockData.blocks': {"$elemMatch":{ "id":card['id']}} },
                            { "$inc": { 'blockData.blocks.$.meta.impressions': 1} })
            except Exception as e:
                print(str(e))
                updateErrorNotifiation(kwargs,{'title': "Google Sheet card", 'message': 'your sheet entry is not updated'})
    return card

def assignAttributeTemplate(kwargs,template_data,inputText):
    """@assignAttributeTemplate assign attribute in builk
    """
    isCardExecuted = False
    if 'attributes' in template_data:
        attributes = list()
        totalAttribute = 0
        attributesData = list()
        if isinstance(template_data['attributes'],list):
            for doc in template_data['attributes']:
                if 'attribute' in doc and 'value' in doc:
                    if doc['attribute'] not in attributes:
                        if "{{" in doc['value'] and "}}" in doc['value']:
                            doc['value'] = fetchUserDeatilsViaCode(kwargs, doc['value'], inputText, newAttributes= attributesData)
                        doc['value'] = arithmaticOperation(doc['value'])
                        attributesData.append({'key': doc['attribute'], 'value': doc['value']})
                        attributes.append(doc['attribute'])
            totalAttribute = len(attributes)
            if totalAttribute:
                subscriberInDb = db.guestUsers.find_one({'_id': ObjectId(kwargs['guestUserId'])}, { 'attributes': 1})
                if subscriberInDb and subscriberInDb is not None:
                    if 'attributes' in subscriberInDb:
                        for doc in subscriberInDb['attributes']:
                            if 'key' in doc:
                                if doc['key'] not in attributes:
                                    attributesData.append(doc)
                                else:
                                    for attrToAssign in attributesData:
                                        if attrToAssign['key'] == doc['key'] and attrToAssign['value']:
                                            if len(attrToAssign['value']) and attrToAssign['value'][0] in ['/', '*', '+', '-']:
                                                if doc['value'].isnumeric() or doc['value'][1:].isnumeric():
                                                    attrToAssign['value'] = str(eval(str(doc['value'])+attrToAssign['value']))
                                                else:
                                                    attrToAssign['value'] = attrToAssign['value'][1:]
                    db.guestUsers.update_one({'_id': subscriberInDb['_id']},{'$set': {'attributes': attributesData[::-1]}})
                    tagInDb = db.pagesMeta.find_one({'botId': kwargs['botId'], 'pageId': kwargs['pageId']},{'attributes': 1})
                    if tagInDb and tagInDb is not None:
                        if 'attributes' in tagInDb:
                            attributes = attributes + tagInDb['attributes']
                            attributes = list(set(attributes))
                    db.pagesMeta.update_one({'botId': kwargs['botId'], 'pageId': kwargs['pageId']},{'$set': {'attributes': attributes}})
                    isCardExecuted = True
    return isCardExecuted

def arithmaticOperation(val):
    try:
        if not re.search('[^\W\d_]',val):            
            splitted = re.findall(r'[+-/*//()]|\d+', val)
            if len(splitted):
                assignSign = ""
                if splitted[0] in ['/','*','+','-']:
                    assignSign = splitted.pop(0)
                return assignSign + str(eval("".join(splitted)))
    except Exception as e:
        print('method: arithmaticOperation')
        print(e,val)
    return val
        

def initiateRedirectTemplate(kwargs, template_data, inputText):
    validUser = True
    isCardExecuted = False
    if 'payload' in template_data and kwargs['redirect_block'] < 5:
        payload = template_data['payload']
        if isValidObjectId(payload):
            if 'doWhen' in template_data:
                validUser = getFilterAudience(template_data['doWhen'],kwargs['pageId'], kwargs['sender_id'])
            if validUser:
                fetchBlock = db.userBlocks.find_one({'botId': kwargs['botId'], '_id':ObjectId(payload)})
                if fetchBlock and fetchBlock is not None:
                    kwargs['redirect_block'] = kwargs['redirect_block'] + 1
                    initiateStart( kwargs, fetchBlock, payload , inputText )
                    isCardExecuted = True
    return isCardExecuted

def initiateStart(  accessibleData, blockData, payload , inputText, isDefault = False):
    """@initiateStart is used to send Default 
    Block of the selected chat.
    """
    accessibleData['defaultSent'] = isDefault
    return fetchTemplateData( accessibleData, blockData, payload, inputText)

def fetchTemplateData(  accessibleData, blockData, payload, inputText):
    """@fetchTemplateData is used to send all the types of templates 
    responses like buttons, quick reply, lists etc.
    """
    if 'blockData' in blockData:
        if 'blocks' in blockData['blockData']:
            if blockData['blockData']['blocks'] and len(blockData['blockData']['blocks']) and blockData['blockData']['blocks'] is not None:
                kwargs = accessibleData
                queueToInsert = {
                    'argsData':kwargs,
                    'blockData':blockData['blockData']['blocks'],
                    'inputText':inputText,
                    'messageType':'template',
                    'queueTime':str(datetime.now())
                }
                sendToMessageParserQueue(queueToInsert)
            else:
                handleDefaultReply( accessibleData, Default_not_understand, inputText)
        else:
            handleDefaultReply( accessibleData, Default_not_understand, inputText)
    else:
        handleDefaultReply( accessibleData, Default_bmw_response, inputText)
    return

def handleDefaultReply( accessibleData, message, inputText):
    """@handleDefaultReply is used for 
    sending Default messages.
    """
    if inputText is not None:
        if inputText.lower() == 'unsubscribe' and accessibleData['alreadyIsSubscribed']:
            handleTextMesaages(accessibleData, message, inputText, "static",True)
            return
    if not accessibleData['defaultSent']:
        default = True
        fetchBlock = db.userBlocks.find_one({'botId': accessibleData['botId'], 'blockData.responseType':'default', 'blockData.isNonRemoval':True})
        if fetchBlock and fetchBlock is not None and accessibleData['isSubscribed']:
            if 'blockData' in fetchBlock:
                if 'blocks' in fetchBlock['blockData']:
                    if len(fetchBlock['blockData']['blocks']):
                        default = False
                        initiateStart( accessibleData, fetchBlock, 'chatbot' ,inputText, True)
                        sendemailOnSendDefaultMessage(accessibleData, inputText)
    return

def sendemailOnSendDefaultMessage(accessibleData, inputText):
    sendData = {'type':'default_message',
                  'data':{'accessibleData': accessibleData, 'inputText': inputText}}
    try:
        client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:901832273167:function:cbb-email-manager',
            InvocationType='Event',
            Payload=json.dumps(sendData)
        )
    except Exception as e:
        print(str(e))

def getAdminDataByBotId(botId):
    userData = None
    botAdminInDb = db.bots.find_one({'_id': ObjectId(botId)},{'admin_id': 1})
    if botAdminInDb and botAdminInDb is not None:
        if 'admin_id' in botAdminInDb:
            userInDb = db.users.find_one({'admin_id': botAdminInDb['admin_id']})
            if userInDb and userInDb is not None:
                userData = userInDb
    return userData

def handleTextMesaages(accessibleData, message, inputText, responseType, isDefault = False):
    """@handleTextMesaages is used to Handle Text messages.
    It save the data into queue to handle respomses."""
    accessibleData['defaultSent'] = isDefault
    kwargs = accessibleData
    queueToInsert = {
        'argsData':kwargs,
        'blockData':{ 
            'message': message,
            'responseType': responseType
            },
        'inputText':inputText,
        'messageType':'text',
        'queueTime':str(datetime.now())
    }
    sendToMessageParserQueue(queueToInsert)
    return


def getFilterAudience(targeting,pageId,userFbId):
    parms = {'pageId': pageId, 'details.isSubscribed': True,'details.id':{'$exists':True}, 'userFbId': userFbId}
    saveFinalvalue =list()
    joinOprator = ['or', 'and']
    operator = ['is','is_not','startwith', '24window']
    orOperator = list()
    andOprator = list()
    counter = 0
    count = 0
    if isinstance(targeting,list):
        targetingArraySize = len(targeting)
        for instance in targeting:
            counter = counter + 1
            if 'qualify' in instance and 'joinWith' in instance and 'operator' and instance and 'operand' in instance:
                if 'value' in instance['qualify'] and 'key' in instance['qualify'] and instance['joinWith'].lower() in joinOprator and instance['operator'].lower() in operator:
                    operand = instance['operand'].strip()
                    if operand is not None and ((isinstance(instance['qualify']['value'],list) and not instance['operator'] == 'startWith') or isinstance(instance['qualify']['value'],str) or isinstance(instance['qualify']['value'],int)):
                        if not instance['operator'] == 'startWith':
                            instance['qualify']['value'] = list(set(instance['qualify']['value']))
                        if instance['qualify'] not in saveFinalvalue or not (operand.lower() == 'attributes' and instance['qualify']['key'].lower() == 'cbb_within 24h window'):
                            saveFinalvalue.append(instance['qualify'])
                            if targetingArraySize == 1:
                                if operand.lower() == 'attributes':
                                    if instance['qualify']['key'].lower() == 'cbb_within 24h window':
                                        isInclude = False
                                        isExclude = False
                                        if instance['operator'] == 'is_not':
                                            for doc in instance['qualify']['value']:
                                                if doc.lower() == 'yes' and not isInclude:
                                                    isInclude = True
                                                else:
                                                    isExclude = True
                                            if isInclude:
                                                isInclude = False
                                            if isExclude:
                                                isExclude = False
                                            if isInclude and isExclude:
                                                parms.update({'lastRecieved': {'$lte': int(datetime.now().strftime("%s") )}})
                                            elif isExclude:
                                                parms.update({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                            else:
                                                parms.update({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                        else:
                                            for doc in instance['qualify']['value']:
                                                if doc.lower() == 'yes' and not isInclude:
                                                    isInclude = True
                                                else:
                                                    isExclude = True
                                            if isInclude and isExclude:
                                                parms.update({'lastRecieved': {'$lte': int(datetime.now().strftime("%s") )}})
                                            elif isExclude:
                                                parms.update({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                            else:
                                                parms.update({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                    elif instance['qualify']['key'].lower() == 'cbb_source':
                                        if instance['operator'] == 'is':
                                            parms.update({ 'source': { '$in':instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({'source': { '$regex': "^"+instance['qualify']['value']} })
                                        else:
                                            parms.update({ 'source': { '$nin':instance['qualify']['value']}})
                                    elif instance['qualify']['key'].lower() == 'cbb_is_guest_user':
                                        if instance['operator'] == 'is':
                                            parms.update({ 'is_guest_user': { '$in':instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({'is_guest_user': { '$regex': "^"+instance['qualify']['value']} })
                                        else:
                                            parms.update({ 'is_guest_user': { '$nin':instance['qualify']['value']}})
                                    elif instance['qualify']['key'].lower() == 'cbb_ad_id':
                                        if instance['operator'] == 'is':
                                            parms.update({ 'ad_id': { '$in':instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({'ad_id': { '$regex': "^"+instance['qualify']['value']} })
                                        else:
                                            parms.update({ 'ad_id': { '$nin':instance['qualify']['value']}})
                                    elif instance['qualify']['key'].lower() == 'cbb_ref':
                                        if instance['operator'] == 'is':
                                            parms.update({ 'ref': { '$in':instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({'ref': { '$regex': "^"+instance['qualify']['value']} })
                                        else:
                                            parms.update({ 'ref': { '$nin':instance['qualify']['value']}})
                                    elif instance['qualify']['key'].lower() == 'cbb_signup':
                                        if instance['operator'] == 'is':
                                            parms.update({ 'date': { '$in':instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({'date': { '$regex': "^"+instance['qualify']['value']} })
                                        else:
                                            parms.update({ 'date': { '$nin':instance['qualify']['value']}})
                                    elif instance['qualify']['key'].lower() == 'cbb_subscriber id':
                                        subscribersId = []
                                        if not instance['operator'] == 'startWith':
                                            for ids in instance['qualify']['value']:
                                                if isValidObjectId(ids):
                                                    subscribersId.append(ObjectId(ids))
                                        else:
                                            parms.update({ '_id': { '$in': subscribersId}})
                                        if len(subscribersId):
                                            if instance['operator'] == 'is':
                                                parms.update({ '_id': { '$in': subscribersId}})
                                            else:
                                                parms.update({ '_id': { '$nin':subscribersId}})
                                    elif instance['qualify']['key'].lower() == 'cbb_lastseen':
                                        if instance['operator'] == 'is':
                                            parms.update({ 'lastUpdated': { '$in':instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({ 'lastUpdated': instance['qualify']['value']})
                                        else:
                                            parms.update({ 'lastUpdated': { '$nin':instance['qualify']['value']}})
                                    elif instance['qualify']['key'].lower() == 'cbb_messenger id':
                                        if instance['operator'] == 'is':
                                            parms.update({ 'userFbId': { '$in':instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({'userFbId': { '$regex': "^"+instance['qualify']['value']} })
                                        else:
                                            parms.update({ 'userFbId': { '$nin':instance['qualify']['value']}})
                                    else:
                                        if instance['operator'] == 'is':
                                            parms.update({'attributes':{'$elemMatch':{'key':instance['qualify']['key'], 'value': { '$in':instance['qualify']['value']}}}})
                                        elif instance['operator'] == 'startWith':
                                            parms.update({'attributes':{'$elemMatch':{'key': instance['qualify']['key'], 'value': { '$regex': "^"+instance['qualify']['value']}}}})
                                        elif instance['operator'] == 'cbb_assigned':
                                            parms.update({'attributes.key': instance['qualify']['key']})
                                        elif instance['operator'] == 'cbb_not_assigned':
                                            parms.update({'attributes.key': {'$ne': instance['qualify']['key']}})
                                        else:
                                            parms.update({'attributes':{'$elemMatch':{ 'value': { '$nin':instance['qualify']['value']}, '$or':[{'key':{'$ne':instance['qualify']['key']}}]}}})
                                elif operand.lower() == 'tags':
                                    if instance['operator'] == 'is':
                                        parms.update({'labels.customLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                    elif instance['operator'] == 'startWith':
                                        parms.update({ 'labels.customLabels.labelName.value':{ '$regex' :instance['qualify']['value']}})
                                    else:
                                        parms.update({'labels.customLabels.labelId': { '$nin':instance['qualify']['value']}})
                                elif operand.lower() == '24window':
                                    if instance['operator'] == 'is':
                                        parms.update({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                    else:
                                        parms.update({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                else:
                                    if instance['operator'] == 'is':
                                        parms.update({'labels.systemLabels.labelId':{ '$in' : instance['qualify']['value']}})
                                    elif instance['operator'] == 'startWith':
                                        if instance['qualify']['key'].lower() == 'firstname':
                                            instance['qualify']['key'] = 'first_name'
                                        elif instance['qualify']['key'].lower() == 'lastname':
                                            instance['qualify']['key'] = 'last_name'
                                        else:
                                            instance['qualify']['key'] = instance['qualify']['key'].lower()
                                        parms.update({'labels.systemLabels.labelName.key': instance['qualify']['key'], 'labels.systemLabels.labelName.value':{ '$regex' : "^"+instance['qualify']['value']}})
                                    else:
                                        parms.update({'labels.systemLabels.labelId': { '$nin':instance['qualify']['value']}})
                            else:
                                if operand.lower() == 'attributes':
                                    if instance['joinWith'].lower() == 'or' and counter < targetingArraySize:
                                        if len(andOprator):
                                            parms.update({'$and': andOprator})
                                            andOprator = []
                                        if instance['qualify']['key'].lower() == 'cbb_within 24h window':
                                            isInclude = False
                                            isExclude = False
                                            if instance['operator'] == 'is_not':
                                                for doc in instance['qualify']['value']:
                                                    if doc.lower() == 'yes' and not isInclude:
                                                        isInclude = True
                                                    else:
                                                        isExclude = True
                                                if isInclude:
                                                    isInclude = False
                                                if isExclude:
                                                    isExclude = False
                                                if isInclude and isExclude:
                                                    orOperator.append({'lastRecieved': {'$lte': int(datetime.now().strftime("%s") )}})
                                                elif isExclude:
                                                    orOperator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                                else:
                                                    orOperator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                            else:
                                                for doc in instance['qualify']['value']:
                                                    if doc.lower() == 'yes' and not isInclude:
                                                        isInclude = True
                                                    else:
                                                        isExclude = True
                                                if isInclude and isExclude:
                                                    orOperator.append({'lastRecieved': {'$lte': int(datetime.now().strftime("%s") )}})
                                                elif isExclude:
                                                    orOperator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                                else:
                                                    orOperator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                        elif instance['qualify']['key'].lower() == 'cbb_source':    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'source': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'source': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'source': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_is_guest_user':    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'is_guest_user': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'is_guest_user': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'is_guest_user': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_ad_id':    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'ad_id': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'ad_id': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'ad_id': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_ref':    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'ref': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'ref': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'ref': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_signup':    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'date': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'date': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'date': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_lastseen':    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'lastUpdated': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'lastUpdated': instance['qualify']['value']})
                                            else:
                                                orOperator.append({'lastUpdated': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_messenger id':    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'userFbId': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'userFbId': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'userFbId': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_subscriber id':
                                            subscribersId = []
                                            if not instance['operator'] == 'startWith':
                                                for ids in instance['qualify']['value']:
                                                    if isValidObjectId(ids):
                                                        subscribersId.append(ObjectId(ids))
                                            else:
                                                orOperator.append({'_id': instance['qualify']['value']})
                                            if len(subscribersId):    
                                                if instance['operator'] == 'is':
                                                    orOperator.append({'_id': { '$in':subscribersId}})
                                                else:
                                                    orOperator.append({'_id': { '$nin':subscribersId}})
                                        else:    
                                            if instance['operator'] == 'is':
                                                orOperator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$in':instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$regex': "^"+instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'cbb_assigned':
                                                parms.update({'attributes.key': instance['qualify']['key']})
                                            elif instance['operator'] == 'cbb_not_assigned':
                                                parms.update({'attributes.key': {'$ne': instance['qualify']['key']}})
                                            else:
                                                orOperator.append({'attributes':{'$elemMatch':{'key':instance['qualify']['key'], 'value': { '$nin':instance['qualify']['value']}}}})
                                                orOperator.append({'attributes.key':{'$ne':instance['qualify']['key']}})
                                                orOperator.append({'attributes':{'$exists': False}})
                                    elif instance['joinWith'].lower() == 'and' and counter < targetingArraySize:
                                        if len(orOperator):
                                            parms.update({'$or': orOperator})
                                            orOperator = []
                                        if instance['qualify']['key'].lower() == 'cbb_within 24h window':
                                            isInclude = False
                                            isExclude = False
                                            if instance['operator'] == 'is_not':
                                                for doc in instance['qualify']['value']:
                                                    if doc.lower() == 'yes' and not isInclude:
                                                        isInclude = True
                                                    else:
                                                        isExclude = True
                                                if isInclude:
                                                    isInclude = False
                                                if isExclude:
                                                    isExclude = False
                                                if isInclude and isExclude:
                                                    andOprator.append({'lastRecieved': {'$lte': int(datetime.now().strftime("%s") )}})
                                                elif isExclude:
                                                    andOprator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                                else:
                                                    andOprator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                            else:
                                                for doc in instance['qualify']['value']:
                                                    if doc.lower() == 'yes' and not isInclude:
                                                        isInclude = True
                                                    else:
                                                        isExclude = True
                                                if isInclude and isExclude:
                                                    andOprator.append({'lastRecieved': {'$lte': int(datetime.now().strftime("%s") )}})
                                                elif isExclude:
                                                    andOprator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                                else:
                                                    andOprator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                        elif instance['qualify']['key'].lower() == 'cbb_source':    
                                            if instance['operator'] == 'is':
                                                andOprator.append({'source': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'source': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'source': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_is_guest_user':    
                                            if instance['operator'] == 'is':
                                                andOprator.append({'is_guest_user': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'is_guest_user': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'is_guest_user': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_ad_id':    
                                            if instance['operator'] == 'is':
                                                andOprator.append({'ad_id': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'ad_id': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'ad_id': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_ref':    
                                            if instance['operator'] == 'is':
                                                andOprator.append({'ref': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'ref': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'ref': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_signup':    
                                            if instance['operator'] == 'is':
                                                andOprator.append({'date': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'date': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'date': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_lastseen':    
                                            if instance['operator'] == 'is':
                                                andOprator.append({'lastUpdated': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'lastUpdated': instance['qualify']['value']})
                                            else:
                                                andOprator.append({'lastUpdated': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_messenger id':    
                                            if instance['operator'] == 'is':
                                                andOprator.append({'userFbId': { '$in':instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'userFbId': { '$regex': "^"+instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'userFbId': { '$nin':instance['qualify']['value']}})
                                        elif instance['qualify']['key'].lower() == 'cbb_subscriber id':
                                            subscribersId = []
                                            if not instance['operator'] == 'startWith':
                                                for ids in instance['qualify']['value']:
                                                    if isValidObjectId(ids):
                                                        subscribersId.append(ObjectId(ids))
                                            else:
                                                andOprator.append({'_id': instance['qualify']['value']})
                                            if len(subscribersId):    
                                                if instance['operator'] == 'is':
                                                    andOprator.append({'_id': { '$in':subscribersId}})
                                                else:
                                                    andOprator.append({'_id': { '$nin':subscribersId}})
                                        else:
                                            if instance['operator'] == 'is':
                                                andOprator.append({'attributes':{'$elemMatch':{'key':instance['qualify']['key'], 'value': { '$in':instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'attributes':{'$elemMatch':{'key':instance['qualify']['key'], 'value': { '$regex': "^"+instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'cbb_assigned':
                                                parms.update({'attributes.key': instance['qualify']['key']})
                                            elif instance['operator'] == 'cbb_not_assigned':
                                                parms.update({'attributes.key': {'$ne': instance['qualify']['key']}})
                                            else:
                                                andOprator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$nin':instance['qualify']['value']}}}})
                                                parms.update({'$or':[{'attributes.key':{'$ne':instance['qualify']['key']}},{'attributes':{'$exists': False}}]})
                                    else:
                                        if len(orOperator):
                                            if instance['operator'] == 'is':
                                                orOperator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$in':instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$regex': "^"+instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'cbb_assigned':
                                                orOperator.update({'attributes.key': instance['qualify']['key']})
                                            elif instance['operator'] == 'cbb_not_assigned':
                                                orOperator.update({'attributes.key': {'$ne': instance['qualify']['key']}})
                                            else:
                                                orOperator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$nin':instance['qualify']['value']}}}})
                                                orOperator.append({'attributes.key':{'$ne':instance['qualify']['key']}})
                                                orOperator.append({'attributes':{'$exists': False}})
                                        elif len(andOprator):
                                            if instance['operator'] == 'is':
                                                andOprator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$in':instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({'attributes':{'$elemMatch': {'key':instance['qualify']['key'], 'value': { '$regex':instance['qualify']['value']}}}})
                                            elif instance['operator'] == 'cbb_assigned':
                                                andOprator.update({'attributes.key': instance['qualify']['key']})
                                            elif instance['operator'] == 'cbb_not_assigned':
                                                andOprator.update({'attributes.key': {'$ne': instance['qualify']['key']}})
                                            else:
                                                andOprator.append({'attributes':{'$elemMatch':  {'key':instance['qualify']['key'], 'value': { '$nin':instance['qualify']['value']}}}})
                                                parms.update({'$or':[{'attributes.key':{'$ne':instance['qualify']['key']}},{'attributes':{'$exists': False}}]})
                                elif operand.lower() == '24window':
                                    if instance['joinWith'].lower() == 'or' and counter < targetingArraySize:
                                        if len(andOprator):
                                            parms.update({'$and': andOprator})
                                            andOprator = []
                                        if instance['operator'] == 'is':
                                            orOperator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                        else:
                                            orOperator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                    elif instance['joinWith'].lower() == 'and' and counter < targetingArraySize:
                                        if len(orOperator):
                                            parms.update({'$or': orOperator})
                                            orOperator = []
                                        if instance['operator'] == 'is':
                                            andOprator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                        else:
                                            andOprator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                    else:
                                        if len(orOperator):
                                            if instance['operator'] == 'is':
                                                orOperator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                            else:
                                                orOperator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                        elif len(andOprator):
                                            if instance['operator'] == 'is':
                                                andOprator.append({'lastRecieved': {'$gte': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                            else:
                                                andOprator.append({'lastRecieved': {'$lt': int(datetime.now().strftime("%s") )- 60*60*24*1}})
                                elif operand.lower() == 'tags':
                                    if instance['joinWith'].lower() == 'or' and counter < targetingArraySize:
                                        if len(andOprator):
                                            parms.update({'$and': andOprator})
                                            andOprator = []
                                        if instance['operator'] == 'is':
                                            orOperator.append({'labels.customLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            orOperator.append({ 'labels.customLabels.labelName.value':{ '$regex' : "^"+instance['qualify']['value']}})
                                        else:
                                            orOperator.append({'labels.customLabels.labelId':  { '$nin':instance['qualify']['value']}})
                                    elif instance['joinWith'].lower() == 'and' and counter < targetingArraySize:
                                        if len(orOperator):
                                            parms.update({'$or': orOperator})
                                            orOperator = []
                                        if instance['operator'] == 'is':
                                            andOprator.append({'labels.customLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            andOprator.append({ 'labels.customLabels.labelName.key': instance['qualify']['key'], 'labels.customLabels.labelName.value':{ '$regex' : "^"+instance['qualify']['value']}})
                                        else:
                                            andOprator.append({'labels.customLabels.labelId': { '$nin':instance['qualify']['value']}})
                                    else:
                                        if len(orOperator):
                                            if instance['operator'] == 'is':
                                                orOperator.append({'labels.customLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                orOperator.append({'labels.customLabels.labelId':{ '$regex' : "^"+instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'labels.customLabels.labelId': { '$nin':instance['qualify']['value']}})
                                        elif len(andOprator):
                                            if instance['operator'] == 'is':
                                                andOprator.append({'labels.customLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                andOprator.append({ 'labels.customLabels.labelName.key': instance['qualify']['key'], 'labels.customLabels.labelName.value':{ '$regex' : "^"+instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'labels.customLabels.labelId': { '$nin':instance['qualify']['value']}})
                                else:
                                    if instance['joinWith'].lower() == 'or' and counter < targetingArraySize:
                                        if len(andOprator):
                                            parms.update({'$and': andOprator})
                                            andOprator = []
                                        if instance['operator'] == 'is':
                                            orOperator.append({'labels.systemLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            if instance['qualify']['key'].lower() == 'firstname':
                                                instance['qualify']['key'] = 'first_name'
                                            elif instance['qualify']['key'].lower() == 'lastname':
                                                instance['qualify']['key'] = 'last_name'
                                            else:
                                                instance['qualify']['key'] = instance['qualify']['key'].lower()
                                            orOperator.append({'labels.systemLabels.labelName.key': instance['qualify']['key'] , 'labels.systemLabels.labelName.value':{ '$regex' : "^"+instance['qualify']['value']}})
                                        else:
                                            orOperator.append({'labels.systemLabels.labelId': { '$nin':instance['qualify']['value']}})
                                    elif instance['joinWith'].lower() == 'and' and counter < targetingArraySize:
                                        if len(orOperator):
                                            parms.update({'$or': orOperator})
                                            orOperator = []
                                        if instance['operator'] == 'is':
                                            andOprator.append({'labels.systemLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                        elif instance['operator'] == 'startWith':
                                            if instance['qualify']['key'].lower() == 'firstname':
                                                instance['qualify']['key'] = 'first_name'
                                            elif instance['qualify']['key'].lower() == 'lastname':
                                                instance['qualify']['key'] = 'last_name'
                                            else:
                                                instance['qualify']['key'] = instance['qualify']['key'].lower()
                                            andOprator.append({'labels.systemLabels.labelName.key': instance['qualify']['key'], 'labels.systemLabels.labelName.value':{ '$regex' : "^"+instance['qualify']['value']}})
                                        else:
                                            andOprator.append({'labels.systemLabels.labelId': { '$nin':instance['qualify']['value']}})
                                    else:
                                        if len(orOperator):
                                            if instance['operator'] == 'is':
                                                orOperator.append({'labels.systemLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                if instance['qualify']['key'].lower() == 'firstname':
                                                    instance['qualify']['key'] = 'first_name'
                                                elif instance['qualify']['key'].lower() == 'lastname':
                                                    instance['qualify']['key'] = 'last_name'
                                                else:
                                                    instance['qualify']['key'] = instance['qualify']['key'].lower()
                                                orOperator.append({'labels.systemLabels.labelName.key': instance['qualify']['key'], 'labels.systemLabels.labelName.value':{ '$regex' :instance['qualify']['value']}})
                                            else:
                                                orOperator.append({'labels.systemLabels.labelId': { '$nin':instance['qualify']['value']}})
                                        elif len(andOprator):
                                            if instance['operator'] == 'is':
                                                andOprator.append({'labels.systemLabels.labelId':{ '$in' :instance['qualify']['value']}})
                                            elif instance['operator'] == 'startWith':
                                                if instance['qualify']['key'].lower() == 'firstname':
                                                    instance['qualify']['key'] = 'first_name'
                                                elif instance['qualify']['key'].lower() == 'lastname':
                                                    instance['qualify']['key'] = 'last_name'
                                                else:
                                                    instance['qualify']['key'] = instance['qualify']['key'].lower()
                                                andOprator.append({'labels.systemLabels.labelName.key': instance['qualify']['key'], 'labels.systemLabels.labelName.value':{ '$regex' :instance['qualify']['value']}})
                                            else:
                                                andOprator.append({'labels.systemLabels.labelId': { '$nin':instance['qualify']['value']}})
        if len(andOprator):
            parms.update({'$and': andOprator})
        if len(orOperator):
            parms.update({'$or': orOperator})
    getAllAudienceCount = db.guestUsers.aggregate([
        {
            '$match':parms
        },
        {
        '$count': "userFbId"
        }
    ])
    getAllAudienceCount = list(getAllAudienceCount)
    countData = len(getAllAudienceCount)
    if countData:
        count = getAllAudienceCount[0]['userFbId']
    return count

def createSheetHeaderList(header):
    """@createSheetHeaderList for helper to get list of header"""
    headers = list()
    sheetHeader = {
      '{{name}}':'name',
      '{{gender}}':'gender',
      '{{first_name}}': 'first name',
      '{{last_name}}': 'last name',
    #   '{{last_Input}}':'last input',
    #   '{{last_seen}}':'last seen',
      '{{locale}}':'locale',
    #   '{{messenger_id}}':'messenger id',
      '{{ref}}':'ref',
      '{{isSubscribed}}':'isSubscribed',
    #   '{{signup}}':'signup',
      '{{source}}':'source',
      '{{subscriberId}}':'subscriber id',
      '{{timezone}}':'timezone',
    #   '{{profile_pic_url}}': 'profile pic url',
      '{{fbid}}': 'fbid',
      '{{input}}': 'input',
      '{{current_date}}': 'current date',
      '{{current_time}}': 'current time'
    }
    for item in header:
        if item in sheetHeader:
            headers.append(sheetHeader[item])
        elif '{{' in item and '}}' in item:
            item = item.strip('{{')
            item = item.strip('}}')
            headers.append(item)
    return headers

def createObjectToSend(header, kwargs, inputText):
    """@createObjectToSend is used to create the object with values those are selected to export"""
    sendData = []
    guestUserInDb = db.guestUsers.find_one({ 'userFbId': kwargs['sender_id'], 'pageId':kwargs['pageId']})
    if guestUserInDb and guestUserInDb is not None:
        subscriberId = str(guestUserInDb['_id'])
        try:
            for outputAtr in header:
                value = ' '
                if outputAtr in guestUserInDb['details']:
                    value = guestUserInDb['details'][outputAtr]
                    sendData.append(value)
                    continue
                elif outputAtr == 'input':
                    value = inputText
                    sendData.append(value)
                    continue
                elif outputAtr == 'last seen':
                    value = guestUserInDb['lastUpdated'] if 'lastUpdated' in guestUserInDb else str(datetime.now())
                    sendData.append(value)
                    continue
                elif outputAtr == 'isSubscribed':
                    value = guestUserInDb['details']['isSubscribed'] if 'isSubscribed' in guestUserInDb['details'] else True
                    sendData.append(value)
                    continue
                elif outputAtr == 'timezone':
                    value = guestUserInDb['details']['timezone'] if 'timezone' in guestUserInDb['details'] else ''
                    sendData.append(value)
                    continue
                elif outputAtr == 'subscriber id':
                    value = subscriberId
                    sendData.append(value)
                    continue
                elif outputAtr == 'fbid':
                    value = guestUserInDb['userFbId'] if 'userFbId' in  guestUserInDb else "0"
                    sendData.append(value)
                    continue
                elif outputAtr == 'signup':
                    value = guestUserInDb['date'] if 'date' in  guestUserInDb else str(datetime.now())
                    sendData.append(value)
                    continue
                elif outputAtr == 'first name':
                    value = guestUserInDb['details']['first_name'] if 'first_name' in  guestUserInDb['details'] else ""
                    sendData.append(value)
                    continue
                elif outputAtr == 'last name':
                    value = guestUserInDb['details']['last_name'] if 'last_name' in  guestUserInDb['details'] else ""
                    sendData.append(value)
                    continue
                elif outputAtr == 'ref':
                    value = guestUserInDb['ref'] if 'ref' in  guestUserInDb else "direct"
                    sendData.append(value)
                    continue
                elif outputAtr == 'source':
                    value = guestUserInDb['source'] if 'source' in  guestUserInDb else "direct"
                    sendData.append(value)
                    continue
                elif outputAtr == 'current time':
                    now = datetime.now()
                    value = str(now.strftime("%H:%M"))
                    sendData.append(value)
                    continue
                elif outputAtr == 'current date':
                    now = datetime.now()
                    value = str(now.strftime("%B %d, %Y"))
                    sendData.append(value)
                    continue
                else:
                    if 'attributes' in guestUserInDb:
                        for attribute in guestUserInDb['attributes']:
                            if 'key' in attribute and 'value' in attribute:
                                if outputAtr == attribute['key']:
                                    value = attribute['value']
                                    # sendData.append(value) 
                                    break
                        sendData.append(value)
                    else:
                        sendData.append(value)
        except Exception as e:
            print(e)
            sendData = sendData
    return sendData
    

def createObjectToSend_V2(header, kwargs, inputText):
    """@createObjectToSend_V2 is used to create the object with values those are selected to export in v2"""
    sendData = []
    guestUserInDb = db.guestUsers.find_one({ 'userFbId': kwargs['sender_id'], 'pageId':kwargs['pageId']})
    if guestUserInDb and guestUserInDb is not None:
        subscriberId = str(guestUserInDb['_id'])
        try:
            for val in header:
                attrs_in_val = re.findall(r'\{\{.*?\}\}',val)
                if attrs_in_val:
                    for attrWithBraces in attrs_in_val:
                        outputAtr = attrWithBraces.strip('{{').strip('}}')
                        value = ' '
                        if outputAtr in guestUserInDb['details']:
                            value = guestUserInDb['details'][outputAtr]
                        elif outputAtr == 'timestamp':
                            now = datetime.now()
                            value = now.strftime("%s")
                        elif outputAtr == 'input':
                            value = inputText
                        elif outputAtr == 'last_seen':
                            value = guestUserInDb['lastUpdated'] if 'lastUpdated' in guestUserInDb else str(datetime.now())
                        elif outputAtr == 'isSubscribed':
                            value = guestUserInDb['details']['isSubscribed'] if 'isSubscribed' in guestUserInDb['details'] else True
                        elif outputAtr == 'timezone':
                            value = guestUserInDb['details']['timezone'] if 'timezone' in guestUserInDb['details'] else ''
                        elif outputAtr == 'subscriberId':
                            value = subscriberId
                        elif outputAtr == 'fbid':
                            value = guestUserInDb['userFbId'] if 'userFbId' in  guestUserInDb else "0"
                        elif outputAtr == 'signup':
                            value = guestUserInDb['date'] if 'date' in  guestUserInDb else str(datetime.now())
                        elif outputAtr == 'first_name':
                            value = guestUserInDb['details']['first_name'] if 'first_name' in  guestUserInDb['details'] else ""
                        elif outputAtr == 'last_name':
                            value = guestUserInDb['details']['last_name'] if 'last_name' in  guestUserInDb['details'] else ""
                        elif outputAtr == 'ref':
                            value = guestUserInDb['ref'] if 'ref' in  guestUserInDb else "direct"
                        elif outputAtr == 'source':
                            value = guestUserInDb['source'] if 'source' in  guestUserInDb else "direct"
                        elif outputAtr == 'current_time':
                            now = datetime.now()
                            value = str(now.strftime("%H:%M"))
                        elif outputAtr == 'current_date':
                            now = datetime.now()
                            value = str(now.strftime("%B %d, %Y"))
                        else:
                            if 'attributes' in guestUserInDb:
                                for attribute in guestUserInDb['attributes']:
                                    if 'key' in attribute and 'value' in attribute:
                                        if outputAtr == attribute['key']:
                                            value = attribute['value']
                                            # sendData.append(value) 
                                            break
                            #     sendData.append(value)
                            # else:
                        val = val.replace(attrWithBraces, value)
                sendData.append(val)
        except Exception as e:
            print(e)
            sendData = sendData
    return sendData

    

def send_responseForCheckbox(kwargs, post_data):
    """For sending response to Facebook"""
    # post_data['messaging_type'] = "RESPONSE"
    post_data['recipient']['user_ref'] = kwargs['sender_id']
    response = requests.post("https://graph.facebook.com/v12.0/me/messages",
        params={"access_token": kwargs['token']},
        data=json.dumps(post_data),
    headers={'Content-type': 'application/json'})
    # print(vars(response))
    if(response.status_code != 200):
        try:
            data = json.loads(response.text, strict=False)
            db.pages.update_one({"id": kwargs['pageId']}, {"$set": {
            "failures."+str(data['error']['code'])+".last_raised": int(datetime.now().strftime("%s")),
            "failures."+str(data['error']['code'])+".type": data['error']['type'],
            "failures."+str(data['error']['code'])+".message": data['error']['message'],
            "failures."+str(data['error']['code'])+".error_subcode": data['error'].get('error_subcode')
        },
        "$inc":{"failures."+str(data['error']['code'])+".raise_count": 1}})
        except Exception as e:
            print('method: send_responseForCheckbox')
            print(kwargs)
            print(str(e))  
    return response

def updateErrorNotifiation(kwargs,error):
    """@updateErrorNotifiation is to update error in user db
    """
    db.bots.update_one({'_id': kwargs['botId']},{'$addToSet': {'errors': error}})
    return

def webhookTemplate(kwargs, block, inputText):
    """@webhookTemplate is used to call third party Api
    """
    isBreak = False
    if 'request_type' in block and 'url' in block and 'headers' in block:
        if block['request_type'] and block['request_type'] is not None and block['url'] and block['url'] is not None:
            regex = re.compile(
                    r'^(?:http|ftp)s?://' # http:// or https://
                    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                    r'localhost|' #localhost...
                    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                    r'(?::\d+)?' # optional port
                    r'(?:/?|[/?]\S+)$', re.IGNORECASE)
            if re.match(regex, block['url']):
                sendData = {'url': block['url'], 'headers': {'Content-Type': 'application/json'}, 'request_type':block['request_type'].lower()}
                if '{{' in block['url'] and '}}' in block['url']:
                    sendData['url'] = fetchUserDeatilsViaCode(kwargs,  block['url'], inputText)
                if isinstance(block['headers'], dict) and bool(block['headers']) and checkProUser(kwargs):
                    headers = json.dumps(block['headers'])
                    if '{{' in headers and '}}' in headers:
                        headers = fetchUserDeatilsViaCode(kwargs,  headers, inputText) 
                    block['headers'] = json.loads(headers, strict=False)
                    sendData['headers'] = block['headers']
                sendData['headers']['User-Agent'] = 'BotMyWork'
                if block['request_type'].lower() == 'post':
                    if 'urlEncoded' in block and 'params' in block:
                        if isinstance(block['params'], dict):
                            params = json.dumps(block['params'])
                            if '{{' in params and '}}' in params:
                                params = fetchUserDeatilsViaCode(kwargs,  params, inputText) 
                            block['params'] = json.loads(params, strict=False)
                            if isinstance(block['urlEncoded'], bool):
                                if block['urlEncoded']:
                                    sendData['params'] = block['params']
                                else:
                                    sendData['data'] = block['params']
                            else:
                                sendData['data'] = block['params']
                        else:
                            sendData['data'] = {}
                        isBreak = cbbWebhookCall(sendData,kwargs)
                elif block['request_type'].lower() == 'get':
                    isBreak = cbbWebhookCall(sendData,kwargs)
    return isBreak

def cbbWebhookCall(receive,kwargs):
    """@cbbWebhookCall is used to send webhook data to the user's systems."""
    webhhokSend = False
    isBreak = False
    try:
        if receive['request_type'] == 'get':
            requestResponse = requests.get(receive['url'], headers=receive['headers'])
            if requestResponse.status_code >= 200 and requestResponse.status_code <= 299:
                webhhokSend = True
        elif receive['request_type'] == 'post':
            if 'data' in receive:
                requestResponse = requests.post(receive['url'], data =  json.dumps(receive['data']), headers = receive['headers'])
                if requestResponse.status_code >= 200 and requestResponse.status_code <= 299:
                    webhhokSend = True
                if requestResponse.status_code >= 200 and requestResponse.status_code <= 299:
                    webhhokSend = True
                    isBreak = getWebhookCardResponse(kwargs,requestResponse)
            elif 'params' in receive:
                requestResponse = requests.post(receive['url'], params = receive['params'], headers = receive['headers'])
                if requestResponse.status_code >= 200 and requestResponse.status_code <= 299:
                    webhhokSend = True
                    isBreak = getWebhookCardResponse(kwargs,requestResponse)
    except Exception as e:
        print(str(e))
    if not webhhokSend:
        sendemailOnWebhookTemplateFailure(kwargs['botId'],kwargs['pageId'],receive['url'])
        updateErrorNotifiation(kwargs,{'title': "Webhook card", 'message': 'webook card geting faild'})
    return isBreak

def sendemailOnWebhookTemplateFailure(botId,pageId,url):
    sendData = {'type':'Webhook_export_failed', 'data':{'botId': botId, 'pageId': pageId, 'url': url}}
    try:
        client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:901832273167:function:cbb-email-manager',
            InvocationType='Event',
            Payload=json.dumps(sendData)
        )
    except Exception as e:
        print(str(e))


def getWebhookCardResponse(kwargs,requestResponse):
    isBreak = False
    try:
        response = json.loads(requestResponse.text, strict=False)
        if 'data' in response and 'status' in response:
            if response['data'] and response['status']:
                data = response['data']
                if 'sendMessage' in data and 'type' in data:
                    if data['sendMessage'] and data['type'] == 'block_id':
                        if 'block_id' in data:
                            if isValidObjectId(data['block_id']):
                                payload = data['block_id']
                                fetchBlock = db.userBlocks.find_one({'_id': ObjectId(data['block_id']), 'botId': kwargs['botId']})
                                if fetchBlock and fetchBlock is not None:
                                    initiateStart( kwargs, fetchBlock, payload , None )
                                    isBreak = True
                    elif data['sendMessage'] and data['type'] == 'blocks':
                        if 'blocks' in data:
                            if isinstance(data['blocks'],list):
                                sendWebhookCardResponseMessage(kwargs,data['blocks'])
                                isBreak = True
    except:
        pass
    return isBreak

def sendWebhookCardResponseMessage(kwargs,blocks):
    try:
        if len(blocks):
            blockCount = 0
            stop = False
            totalBlocks = len(blocks)
            storeLastMessage = {}
            storeType = {}
            totalBlocks = len(blocks)
            for block in blocks:
                blockCount = blockCount + 1
                execute = True
                if stop or blockCount > 5:
                    break
                postData = {'message':{}, 'recipient':{}}
                savepostData = {'message':{}}
                current = {}
                block_type = block['template_type']
                if block_type == 'quick_reply':
                    postData['message'], current['type'], setAttribute  = quickReplies(kwargs, block, None)
                    savepostData = {
                        'message': postData['message'],
                        'type': current['type']
                    }
                    if setAttribute and setAttribute is not None:
                        savepostData['attribute'] = setAttribute
                    if storeLastMessage and storeLastMessage is not None:
                        if storeType['type'] == 'text':
                            postData['message']['text'] = storeLastMessage['text']
                        else:
                            break
                    else:
                        break
                elif block_type == 'buttons':
                    postData['message'], current['type'] = buttonTemplate(kwargs, block, None)
                    savepostData = {
                        'message': postData['message'],
                        'type': current['type']
                    }
                elif block_type == 'media':
                    postData['message'], current['type'] = imageTemplateForWebhookResponseCard(kwargs, block)
                    savepostData = {
                        'message': postData['message'],
                        'type': current['type']
                    }
                if blockCount < totalBlocks:
                    if blocks[blockCount]['template_type'] == 'quick_reply':
                        storeLastMessage = postData['message']
                        storeType['type'] = current['type']
                        execute = False
                    
                if postData['message'] and postData['message'] is not None and execute:
                    if current['type'] == 'quickReply':
                        stop = True
                    sendStatus = send_response(kwargs, postData)
                    if sendStatus.status_code == 200:
                        db.guestUsers.update_one({'userFbId': kwargs['sender_id']},
                            {'$set': { 'lastMessage' : savepostData}})
    except Exception as e:
        print(e)
    return

def imageTemplateForWebhookResponseCard(kwargs,card):
    
    """@imageTemplateForWebhookResponseCard is used to build json of media 
    type and called from @fetchTemplateData.
    """
    postBack = {}        
    templates = []
    returnType = 'media'
    if 'template_type' in card and 'image' in card:
        if 'url' in card['image'] and card['template_type']== 'media':
            if isinstance(card['image']['url'],str):
                url = card['image']['url']
                if url.startswith("https://"):
                    attachmentId = generateAttachmentId(kwargs['token'], url, 'image')
                    if attachmentId:
                        inset = {
                            "media_type":'image',
                            "attachment_id":attachmentId
                        }
                        templates.append(inset)
        if len(templates):
            postBack = {
                "attachment": {
                    "type":"template",
                    "payload": {
                        "template_type":"media",
                        "elements": templates
                    }
                }
            }
    return postBack, returnType

def clearValidationTemplate(kwargs, card, inputText):
    isExecuted = False
    if 'isActive' in card:
        if card['isActive'] == 'true' or card['isActive'] == True:
            db.guestUsers.update_one({'_id': ObjectId(kwargs['guestUserId'])},{'$set': {'lastMessage': {}}})
            isExecuted = True
    return isExecuted

def mediaTemplate(kwargs,  template_data, inputText):
    """@mediaTemplate is used to build json of media 
    type and called from @fetchTemplateData.
    """
    postBack = {}        
    templates = []
    returnType = 'media'
    for data in template_data['elements']:
        if 'attachment_id' in data and 'media_type' in data:
            if data['media_type'] == 'image' or data['media_type'] == 'video' and data['attachment_id'] and data['attachment_id'] is not None:
                attachmentUrl = getImageUrl(data['attachment_id'])
                attachmentId = generateAttachmentId(kwargs['token'], attachmentUrl, data['media_type'])
                if attachmentId:
                    inset = {
                        "media_type":data['media_type'],
                        "attachment_id":attachmentId
                    }
                    templates.append(inset)
    if len(templates):
        postBack = {
            "attachment": {
                "type":"template",
                "payload": {
                    "template_type":"media",
                    "elements": templates
                }
            }
        }
    return postBack, returnType

def quickReplies(kwargs, template_data, inputText):
    """@quickReplies is used to build json of Quick Reply 
    type and called from @fetchTemplateData.
    """
    postBack = {}
    quick_replies = []
    setAttribute = None
    quickReply = template_data
    returnType = 'quickReply'
    if 'attribute' in template_data:
        if template_data['attribute'] and template_data['attribute'] is not None:
            attribute = template_data['attribute'].strip()
            if attribute and attribute is not None:
                setAttribute = attribute
    if 'buttons' in quickReply:
        for data in quickReply['buttons']:
            if 'type' in data:
                if data['type'] == 'postback' and 'payload' in data and 'title' in data:
                    productTitle = data['title'] if data['title'] and data['title'] is not None else Default_title
                    payload1 = data['payload'] if data['payload'] and data['payload'] is not None else Default_payload
                    quick_replies.append({
                        'content_type': 'text',
                        'title': productTitle,
                        'payload': createPayload(config['CBBQUICKREPLYPAYLOADPREFIX'], payload1)
                    })
        if quick_replies and quick_replies is not None:
            postBack['quick_replies'] = quick_replies
    return postBack ,returnType, setAttribute

def fetchUserDeatilsViaCode(kwargs,  text_data, inputText=None, newAttributes = []):
    """@fetchUserDeatilsViaCode is used to replace 
    the keywords with values.
    """
    guestuser = fetchGuestUserById({'pageId':kwargs['pageId'], 'userFbId':kwargs['sender_id']})
    if guestuser and guestuser is not None:
        if 'details' in guestuser:
            if "{{first_name}}" in text_data and 'first_name' in guestuser['details']:
                text_data = text_data.replace("{{first_name}}", guestuser['details']['first_name'])
            if "{{last_name}}" in text_data and 'last_name' in guestuser['details']:
                text_data = text_data.replace("{{last_name}}", guestuser['details']['last_name'])
            if "{{name}}" in text_data and 'name' in guestuser['details']:
                text_data = text_data.replace("{{name}}", guestuser['details']['name'])
            if "{{gender}}" in text_data and 'gender' in guestuser['details']:
                text_data = text_data.replace("{{gender}}", guestuser['details']['gender'])
            if "{{locale}}" in text_data and 'locale' in guestuser['details']:
                text_data = text_data.replace("{{locale}}", guestuser['details']['locale'])
            if "{{timezone}}" in text_data and 'timezone' in guestuser['details']:
                text_data = text_data.replace("{{timezone}}", str(guestuser['details']['timezone']))
            if "{{isSubscribed}}" in text_data and 'isSubscribed' in guestuser['details']:
                text_data = text_data.replace("{{isSubscribed}}", str(guestuser['details']['isSubscribed']))
            if "{{fbid}}" in text_data and 'id' in guestuser['details']:
                text_data = text_data.replace("{{fbid}}", guestuser['details']['id'])
            if "{{input}}" in text_data and inputText:
                text_data = text_data.replace("{{input}}", inputText)
            if "{{profile_pic_url}}" in text_data and 'profile_pic' in guestuser['details']:
                text_data = text_data.replace("{{profile_pic_url}}", guestuser['details']['profile_pic']) 
        if 'attributes' in guestuser:
            if len(guestuser['attributes']):
                guestuser['attributes'] = [*newAttributes, *guestuser['attributes']]
        else:
            guestuser['attributes'] = newAttributes
        for attribute in guestuser['attributes']:
            if "{{"+attribute['key']+"}}" in text_data:
                text_data = text_data.replace("{{"+attribute['key']+"}}", str(attribute['value']))
    else:
        if "{{first_name}}" in text_data:
            text_data = text_data.replace("{{first_name}}", "")
        if "{{last_name}}" in text_data:
            text_data = text_data.replace("{{last_name}}", "")
        if "{{name}}" in text_data:
            text_data = text_data.replace("{{name}}", "")
        if "{{input}}" in text_data:
            text_data = text_data.replace("{{input}}", "")
    if "{{subscriberId}}" in text_data and '_id' in guestuser:
        text_data = text_data.replace("{{subscriberId}}", str(guestuser['_id']))
    if "{{source}}" in text_data and 'source' in guestuser:
        text_data = text_data.replace("{{source}}", guestuser['source'])
    if "{{current_date}}" in text_data:
        now = datetime.now()
        current_date = str(now.strftime("%B %d, %Y"))
        text_data = text_data.replace("{{current_date}}", current_date)
    if "{{current_time}}" in text_data:
        now = datetime.now()
        current_time = str(now.strftime("%H:%M"))
        text_data = text_data.replace("{{current_time}}", current_time)
    if 'botId' in  kwargs and kwargs['botId'] is not None:
        if isValidObjectId(kwargs['botId']):
            bot = db.bots.find_one({"_id": ObjectId(kwargs['botId'])},{"globalAttributes":1})
            if bot and 'globalAttributes' in bot and bot['globalAttributes'] is not None:
                if len(bot['globalAttributes']):
                    for globalAttr in bot['globalAttributes']:
                        if "{{"+globalAttr['key']+"}}" in text_data:
                                text_data = text_data.replace("{{"+globalAttr['key']+"}}", str(globalAttr['defaultValue']))
    return text_data 

def notifyToAdminViaEmailTemplate(kwargs, card, inputText):
    sendData = {'type':'send_email_card', 'data':{'kwargs': kwargs, 'card': card, 'inputText': inputText}}
    try:
        client.invoke(
            FunctionName='arn:aws:lambda:us-east-1:901832273167:function:cbb-email-manager',
            InvocationType='Event',
            Payload=json.dumps(sendData)
        )
    except Exception as e:
        print(str(e))
               
def notifyToZapier(kwargs):
    """@notifyToZapier is used to notify the data to the zapier section"""
    sessionRequest.post(config['APIACCESSURL']+"/zapier/webhook",
        data=json.dumps(kwargs),
        headers={'Content-type': 'application/json'})
    return

def subsribeSequenceTemplate(kwargs, card, inputText):
    isExecuted = False
    if 'sequence_id' in card:
        if isValidObjectId(card['sequence_id']):
            isExecuted = True
            guestUsersIndb = db.guestUsers.find_one({'_id': ObjectId(kwargs['guestUserId']),
            'sequences':{"$elemMatch":{ "id": card['sequence_id']}}})
            if guestUsersIndb is None:
                db.guestUsers.update_one({'_id': ObjectId(kwargs['guestUserId'])},
                {'$addToSet': {'sequences': {'id': card['sequence_id'], 'index': -1, 'lastSend': str(datetime.now()), 'actualSendingAt':  str(datetime.now())}}})
            else:
                db.guestUsers.update_one({'_id': ObjectId(kwargs['guestUserId']), 'sequences':{"$elemMatch":{ "id": card['sequence_id']}}},
                {'$set': {'sequences.$.index': -1, 'sequences.$.lastSend': str(datetime.now()), 'sequences.$.actualSendingAt': str(datetime.now())}})
    return isExecuted

def unsubsribeSequenceTemplate(kwargs, card, inputText):
    isExecuted = False
    if 'sequence_id' in card:
        if isValidObjectId(card['sequence_id']) or card['sequence_id'].lower() == 'all':
            isExecuted = True
            if card['sequence_id'].lower() == 'all':
                db.guestUsers.update_one({'_id': ObjectId(kwargs['guestUserId'])},
                    {'$set': {'sequences': []}})
            else:
                guestUsersIndb = db.guestUsers.find_one({'_id': ObjectId(kwargs['guestUserId']),
                'sequences':{"$elemMatch":{ "id": card['sequence_id']}}})
                if guestUsersIndb and guestUsersIndb is not None:
                    if isValidObjectId(card['sequence_id']):
                        db.guestUsers.update_one({'_id': ObjectId(kwargs['guestUserId'])},
                        {'$pull': {'sequences': {'id': card['sequence_id']}}})
                   
    return isExecuted

def sendToMessageParserQueue(queueToInsert, delay=0):
    sqs = boto3.client(
            'sqs',
            # endpoint_url='http://'+os.environ.get('LOCALSTACK_HOSTNAME')+':4566', #for localstack
            aws_access_key_id=os.environ.get('CBB_AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('CBB_AWS_ACCESS_KEY'),
            # aws_session_token=SESSION_TOKEN
        )
    sqs.send_message(QueueUrl=os.environ.get('PARSED_MESSAGE_QUEUE_URL'),
                    MessageBody=json.dumps(queueToInsert),
                    DelaySeconds=0,
                    MessageDeduplicationId=str(ObjectId()),
                    MessageGroupId=queueToInsert['argsData']['sender_id'],
                    MessageAttributes={
                        "contentType": {
                            "StringValue": "application/json", "DataType": "String"}
                    }
                )

def sendToMessageTypingQueue(queueToInsert, delay=0):
    sqs = boto3.client(
            'sqs',
            #  endpoint_url='http://'+os.environ.get('LOCALSTACK_HOSTNAME')+':4566', #for localstack
            aws_access_key_id=os.environ.get('CBB_AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('CBB_AWS_ACCESS_KEY'),
            # aws_session_token=SESSION_TOKEN
        )
    # https://sqs.us-east-1.amazonaws.com/558831136623/TypingQueue
    sqs.send_message(QueueUrl=os.environ.get('TYPING_QUEUE_URL'),
                    MessageBody=json.dumps(queueToInsert),
                    DelaySeconds=delay,
                    # MessageDeduplicationId=str(ObjectId()),
                    MessageAttributes={
                        "contentType": {
                            "StringValue": "application/json", "DataType": "String"}
                    }
                )
    
def send_response(kwargs, post_data):
    """For sending response to Facebook"""
    post_data['messaging_type'] = "RESPONSE"
    post_data['recipient']['id'] = kwargs['sender_id']
    response = requests.post("https://graph.facebook.com/v12.0/me/messages",
        params={"access_token": kwargs['token']},
        data=json.dumps(post_data),
    headers={'Content-type': 'application/json'})
    # print(vars(response))
    if(response.status_code != 200):
        try:
            data = json.loads(response.text, strict=False)
            db.pages.update_one({"id": kwargs['pageId']}, {"$set": {
            "failures."+str(data['error']['code'])+".last_raised": int(datetime.now().strftime("%s")),
            "failures."+str(data['error']['code'])+".type": data['error']['type'],
            "failures."+str(data['error']['code'])+".message": data['error']['message'],
            "failures."+str(data['error']['code'])+".error_subcode": data['error'].get('error_subcode')
        },
        "$inc":{"failures."+str(data['error']['code'])+".raise_count": 1}})
        except Exception as e:
            print('method: send_response')
            print(kwargs, post_data)
            print(str(e))        
    return response

def updateGuestChatHandover(kwargs, setData, data):
    try:
        db.guestUsers.update_one(
            { 'userFbId': kwargs['sender_id'], 'pageId': kwargs['pageId'] },
            { '$set': setData })
        # sendToEchoHandlerQueue(kwargs, data)
    except Exception as e:
        print('method: updateGuestChatHandover')
        print(e)

def passThreadControl(kwargs, block):
    """For pass control to another app"""
    post_data = {"recipient":{"id":kwargs['sender_id']}}
    if 'secondary_app_id' in block:
        post_data['target_app_id'] = block['secondary_app_id']
    r = requests.post("https://graph.facebook.com/v12.0/me/pass_thread_control",
                      params={"access_token": kwargs['token']},
                      data=json.dumps(post_data),
                      headers={'Content-type': 'application/json'})
    if(r.status_code != 200):
        try:
            data = json.loads(r.text, strict=False)
            db.pages.update_one({"id": kwargs['pageId']}, {"$set": {
            "failures."+str(data['error']['code'])+".last_raised": int(datetime.now().strftime("%s")),
            "failures."+str(data['error']['code'])+".type": data['error']['type'],
            "failures."+str(data['error']['code'])+".message": data['error']['message'],
            "failures."+str(data['error']['code'])+".error_subcode": data['error'].get('error_subcode')
        },
        "$inc":{"failures."+str(data['error']['code'])+".raise_count": 1}})
        except Exception as e:
            print('method: send_response')
            print(kwargs, post_data)
            print(str(e))        
    return r

def handleActiveCampaign(kwargs, block, inputText):
    """ @handleActiveCampaign is used to handle active campaign card and create new contact on active campaign """
    contactSent = False
    if 'credential_id' in block and 'fields' in block:
        if isValidObjectId(block['credential_id']) and isinstance(block['fields'],list):
            contact = {}
            fieldValues = []
            active_campaign_cred = db.users.find_one({'apps.activeCampaign':{'$elemMatch':{'cbbid': block['credential_id']}}},{'apps.activeCampaign':1})
            if active_campaign_cred is not None:
                active_campaign_cred = active_campaign_cred['apps']['activeCampaign'][0]
                if active_campaign_cred is not None and 'url' in active_campaign_cred and 'token' in active_campaign_cred:
                    fieldValues = [row.get('value', '') for row in block['fields']]
                    if fieldValues:
                        fieldValuesReplaced = createObjectToSend_V2(fieldValues, kwargs, inputText)
                        for i,val in enumerate(fieldValuesReplaced):
                            if 'fieldName' in block['fields'][i]:
                                if 'type' in block['fields'][i]:
                                    if block['fields'][i]['type'] == 'default':
                                        contact[block['fields'][i]['fieldName']] = val
                                    elif block['fields'][i]['type'] == 'custom' and 'id' in block['fields'][i]:
                                        fieldValues.append({'field': str(block['fields'][i]['id']), 'value':val})
                                    else:
                                        contact[block['fields'][i]['fieldName']] = val
                                else:
                                    contact[block['fields'][i]['fieldName']] = val
                    if 'customFields' in block and isinstance(block['customFields'],list):
                        customFieldValues = [row.get('value', '') for row in block['customFields']]
                        if customFieldValues:
                            customValuesReplaced = createObjectToSend_V2(customFieldValues, kwargs, inputText)
                            for i,val in enumerate(customValuesReplaced):
                                if 'id' in block['customFields'][i]:
                                    fieldValues.append({'field': str(block['customFields'][i]['id']), 'value':val})
                    if fieldValues:
                        contact['fieldValues'] = fieldValues
                    dataToSend = {'contact': contact}
                    try:
                        url = active_campaign_cred['url'].strip("/")
                        resp = requests.post(url+'/api/3/contacts', data=json.dumps(dataToSend),
                            headers={'Accept': 'application/json', 'Api-Token': active_campaign_cred['token']})
                        if resp.status_code >= 200 and resp.status_code <300:
                            contactSent = True
                            if 'list' in block and block['list'] is not None and block['list'].isnumeric():
                                ac_contact = json.loads(resp.text, strict=False)
                                if 'contact' in ac_contact and ac_contact['contact'] is not None:
                                    contactList = {
                                        "contactList": {
                                            "list": int(block['list']),
                                            "contact": ac_contact['contact']['id'],
                                            "status": 1
                                        }
                                    }
                                    requests.post(url+'/api/3/contactLists', data=json.dumps(contactList),
                                        headers={'Accept': 'application/json', 'Api-Token': active_campaign_cred['token']})
                    except Exception as e:
                        print(e)
                    if not contactSent:
                        pass
    return contactSent
                #Send mail here if contact creation failed


def handleUserLevelPeristMenu(kwargs, block):
    """ @handleUserLevelPeristMenu is used to set user level persist menu """
    menus = []
    input_disabled = False
    locale = 'default'
    updated = False
    for item in block.get('callToActions',[]):
        if 'type' in item and 'title' in item and ('payload' in item or 'url' in item):
            if item['type'] == 'postback':
                menuItem = {
                    "type": "postback",
                    "title": item['title'],
                    "payload": item['payload']
                }
                menus.append(menuItem)
            elif item['type'] == 'web_url':
                menuItem = {
                    "type": "web_url",
                    "title": item['title'],
                    "payload": item['url']
                }
                if 'webview_height_ratio' in item and item['webview_height_ratio'] in ['full', 'tall', 'compact']:
                    menuItem['webview_height_ratio'] = item['webview_height_ratio']
                menus.append(menuItem)
    if menus:
        if 'input_disabled' in block and isinstance(block['input_disabled'], bool):
            input_disabled = block['input_disabled']
        if 'locale' in block and block['locale'] is not None:
            locale = block['locale']
        postData = {
            "psid": kwargs['sender_id'],
            "persistent_menu": [
                    {
                        "locale": locale,
                        "composer_input_disabled": input_disabled,
                        "call_to_actions": menus
                    }
                ]
            }
        resp = requests.post("https://graph.facebook.com/v12.0/me/custom_user_settings",
                    params={"access_token": kwargs['token']}, data=json.dumps(postData), 
                    headers={'Content-Type': 'application/json'})
        if resp.status_code == 200:
            updated = True
        else:
            try:
                data = json.loads(resp.text,strict=False)
                db.pages.update_one({"id": kwargs['pageId']}, {"$set": {
                "failures."+str(data['error']['code'])+".last_raised": int(datetime.now().strftime("%s")),
                "failures."+str(data['error']['code'])+".type": data['error']['type'],
                "failures."+str(data['error']['code'])+".message": data['error']['message'],
                "failures."+str(data['error']['code'])+".error_subcode": data['error'].get('error_subcode')
                    },
                "$inc":{"failures."+str(data['error']['code'])+".raise_count": 1}})
            except Exception as e:
                print('method: handleUserLevelPeristMenu')
                print(kwargs)
                print(str(e)) 
    return updated

def handleRedirectCard(kwargs, block, inputText):
    isCardExecuted = False
    if 'redirects' in block and isinstance(block['redirects'], list) and kwargs['redirect_block'] < 5:
        for redirect in block['redirects']:
            payload = redirect_with_condition(kwargs, redirect)
            if payload and isValidObjectId(payload):
                fetchBlock = db.userBlocks.find_one({'botId': kwargs['botId'], '_id':ObjectId(payload)})
                if fetchBlock and fetchBlock is not None:
                    kwargs['redirect_block'] = kwargs['redirect_block'] + 1
                    initiateStart(kwargs, fetchBlock, payload , inputText)
                    isCardExecuted = True
                    break
    return isCardExecuted
            

def redirect_with_condition(kwargs, redirect):
    payload = False
    if redirect is not None:
        if isValidObjectId(redirect.get('payload')):
            if isinstance(redirect.get('doWhen', False),list):
                joinWith = False
                query = {'pageId': kwargs['pageId'],'details.isSubscribed': True,'details.id':{'$exists':True}, 'userFbId': kwargs['sender_id']}
                and_c, or_c = [], []
                for cond in redirect['doWhen']:
                    if not isinstance(cond, dict):
                        continue                    
                    if joinWith == 'and':
                        create_filter_query(cond, and_c)
                    else:
                        create_filter_query(cond, or_c)
                    if 'joinWith' in cond and cond['joinWith'].lower() in ['and', 'or']:
                        joinWith = cond['joinWith'].lower()
                    else:
                        break                
                if and_c:
                    query.update({'$and': and_c})
                if or_c:
                    query.update({'$or': or_c})
                result = db.guestUsers.aggregate([{'$match':query}])
                if len(list(result)):
                    payload = redirect['payload']
    return payload

def create_filter_query(cond, params = []):
    VALID_OPERATORS = ['is', 'is_not', 'startWith']
    if 'operand' in cond and 'operator' in cond and 'qualify' in cond:
        if isinstance(cond['operator'], str) and isinstance(cond['qualify'],dict) and isinstance(cond['operand'],str):
            if 'key' in cond['qualify'] and 'value' in cond['qualify']:
                if cond['operator'] in VALID_OPERATORS and isinstance(cond['qualify']['key'],str):
                    operand = cond['operand'].lower()
                    validCondition = False
                    if operand == 'fullname':
                        validCondition = validateOperator('details.name', cond['operator'], cond['qualify'])
                    elif operand == 'lastname':
                        validCondition = validateOperator('details.last_name', cond['operator'], cond['qualify'])
                    elif operand == 'firstname':
                        validCondition = validateOperator('details.first_name', cond['operator'], cond['qualify'])
                    elif operand == 'gender':
                        validCondition = validateOperator('details.gender', cond['operator'], cond['qualify'])
                    elif operand == 'locale':
                        validCondition = validateOperator('details.locale', cond['operator'], cond['qualify'])
                    elif operand == 'timezone':
                        validCondition = validateOperator('details.timezone', cond['operator'], cond['qualify'])
                    elif operand == 'tags':
                        validCondition = validateConditionForTags(cond['operator'], cond['qualify'])
                    elif operand == 'attributes':
                        validCondition = validateConditionForAttribute(cond['operator'], cond['qualify'])
                    if validCondition:
                            params.append(validCondition)

def validateOperator(matchKey,operator, qualify):
    STATIC_OPERATORS = {'is': '$in', 'is_not': '$nin'}
    if operator in STATIC_OPERATORS and isinstance(qualify['value'],list):
        return {matchKey: {STATIC_OPERATORS[operator]: list(set(qualify['value']))}}
    elif operator.lower() == 'startwith' and isinstance(qualify['value'],str):
        return {matchKey: {'$regex': "^"+qualify['value']}}
    return False

def validateConditionForAttribute(operator, qualify):
    STATIC_OPERATORS = {'is': '$in', 'is_not': '$nin'}
    STATIC_OPERATORS_24 = {'is': {'yes': '$gt', 'no': '$lte'}, 'is_not': {'yes':'$lte', 'no': '$gt'}}
    if qualify['key'].lower().startswith('cbb_'):
        key = qualify['key'].lower()
        if key == 'cbb_within 24h window':
            if operator in STATIC_OPERATORS:
                if qualify['value']:
                    match = {'lastRecieved': {}}
                    for val in qualify['value']:
                        if operator == 'is':
                            if val.lower() == 'yes':
                                op = STATIC_OPERATORS_24[operator]['yes']
                            else:
                                op = STATIC_OPERATORS_24[operator]['no']
                            match['lastRecieved'][op] = int(datetime.now().strftime("%s") )- 60*60*24*1
                    return match
                return False
            elif operator.lower() == 'startwith' and isinstance(qualify['value'],str):
                if qualify['value'].lower() == 'yes':
                    return {'lastRecieved':{'$gt':int(datetime.now().strftime("%s") )- 60*60*24*1}}
                else:
                    return {'lastRecieved':{'$lte': int(datetime.now().strftime("%s") )- 60*60*24*1}}
        elif key == 'cbb_source':
            return validateOperator('source', operator, qualify)
        elif key == 'cbb_is_guest_user':
            return validateOperator('is_guest_user', operator, qualify)
        elif key == 'cbb_ad_id':
            return validateOperator('ad_id', operator, qualify)
        elif key == 'cbb_ref':
            return validateOperator('ref', operator, qualify)
        elif key == 'cbb_signup':
            return validateOperator('date', operator, qualify)
        elif key == 'cbb_subscriber id':
            subscribersIds = []
            for ids in qualify['value']:
                if isValidObjectId(ids):
                    subscribersIds.append(ObjectId(ids))
            qualify['value'] = subscribersIds
            return validateOperator('_id', operator, qualify)
        elif key == 'cbb_lastseen':
            return validateOperator('lastUpdated', operator, qualify)
        elif key == 'cbb_messenger id':
            return validateOperator('userFbId', operator, qualify)
    else:
        if operator in STATIC_OPERATORS and isinstance(qualify['value'],list):
            return {'attributes': {'$elemMatch': {'key': qualify['key'], 'value': {STATIC_OPERATORS[operator]: list(set(qualify['value']))}}}}
        elif operator.lower() == 'startwith' and isinstance(qualify['value'],str):
            return {'attributes': {'$elemMatch': {'key': qualify['key'], 'value': {'$regex': "^"+qualify['value']}}}}
        elif operator == 'cbb_assigned':
            return {'attributes.key': qualify['key']}
        elif operator == 'cbb_not_assigned':
            return {'attributes.key': {'$ne': qualify['key']}}
        return False


def validateConditionForTags(operator, qualify):
    """ @validateConditionForTags is used create query for matching tags:
      param: 
        operator: str, 
        qualify: dict
     """
    STATIC_OPERATORS = {'is': '$in', 'is_not': '$nin'}
    if operator in STATIC_OPERATORS and isinstance(qualify['value'],list):
        if qualify['key'] not in ['name', 'first_name', 'last_name', 'gender', 'locale', 'timezone']:
            return {'labels.customLabels.labelId': {STATIC_OPERATORS[operator]: list(set(qualify['value']))}}
        else:
            return {'details.'+qualify['key']: {STATIC_OPERATORS[operator]: list(set(qualify['value']))}}
    elif operator.lower() == 'startwith' and isinstance(qualify['value'],str):
        return { 'labels.customLabels.labelName.value': { '$regex' :qualify['value']}}
    
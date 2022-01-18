config = {
    # 'MONGO_URI': "mongodb+srv://nitendraAdmin:8115843849@cluster0-vcksx.gcp.mongodb.net/chatbotbuilder?retryWrites=true&w=majority",
    'SITEBASEURL': 'https://apps.botmywork.com',  #live
    # 'SITEBASEURL': 'https://192.168.1.47:4400',  #Dev
    'BASEACCESSURL': 'https://webhook.botmywork.com/tqybsaedvv', #live
    # 'BASEACCESSURL': 'https://a9d9e2b8c2f9.ngrok.io', #local
    'ADMINACCESSURL': 'https://api.botmywork.com/tqybsaedvv', #live
    # 'ADMINACCESSURL': 'https://127.0.0.1:5000', #local
    'SOCKETACCESSURL': 'https://apps.botmywork.com/wgeorqhxwq', #live
    # 'SOCKETACCESSURL': 'https://192.168.0.212:5002', #local
    'APIACCESSURL': "https://webhook.botmywork.com/wgeorqhxwq", #Live
    # 'APIACCESSURL': "http://127.0.0.1:5000", #Dev
    'CBBLANDINGPAGE': 'https://botmywork.com/chatbot-builder/',
    'MAX_CONTENT_LENGTH': 1 * 1024 * 1024,
    'MONGOALCHEMY_DATABASE': 'library',
    'UPLOAD_FOLDER': '/uploads',
    'CUSTOMPREFIX': "BMWCustom-",
    'SYSTEMPREFIX': "BMWSystem-",
    'CBBLISTPAYLOADPREFIX': 'OeOm5ZS7Wc',
    'CBBQUICKREPLYPAYLOADPREFIX': 'DQBMmZcd9J',
    'CBBGALLERYPAYLOADPREFIX': 'rsSphDLyTM',
    'CBBPERSISTMENUPAYLOADPREFIX': '8fVoXaYX4l',
    'CBBBUTTONPAYLOADPREFIX':'cFQv0oV3HG',
    'CBBACTIONPAYLOADPREFIX':'HRBJmIXsvP',
    'CBBMAINPAGE': '193258151335017', #Live
    'TESTINGPAGE': '101535438241136', #Live
    # 'CBBMAINPAGE': '113887726972654', #Dev
    # 'TESTINGPAGE': '102058608168941', #Dev
    # 'FBCLIENTID': '1888647441176483', #DEV
    'FBCLIENTID': '282654415633800',  #LIVE
    # 'FBCLIENTSECRET': '35e706b5f9b775fb839c9a3bd2f93c11', #DEV
    'FBCLIENTSECRET': 'fd0538017b9443065f43ed0244d04ee0',  #LIVE
    'ALLOWED_EXTENSIONS': set(['png', 'jpg', 'jpeg', 'gif']),
    # CORS_HEADERS = 'Content-Type'
    # 'GOOGLE_CLIENT_SECRET': 'eTSiFWP2qzuROOzUebGzEgM_', #dev
    'GOOGLE_CLIENT_SECRET': '65ue_iGspJaGLYpXQ8q2RAcf', #live
    # 'GOOGLE_CLIENT_ID': '495694626848-ol803uhusasufhiv6souel9nhqs1bl3c.apps.googleusercontent.com' #dev
    'GOOGLE_CLIENT_ID': '987871053002-8q7t701o0js8grh8fi9s4lgqceh56nke.apps.googleusercontent.com', #live
    'GOOGLE_TOKEN_URI': 'https://oauth2.googleapis.com/token'

}
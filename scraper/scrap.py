import tempfile
import time

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver


true= True
false = False


cookie_list = [
    {
        "domain": ".lex.pl",
        "expirationDate": 1783761920,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_clck",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "80hgxv%7C2%7Cfxi%7C0%7C2015",
        "id": 1
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1752323084,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_clsk",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "k40nwr%7C1752236684424%7C2%7C1%7Ca.clarity.ms%2Fcollect",
        "id": 2
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1760005159,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_fbp",
        "path": "/",
        "sameSite": "lax",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "fb.1.1751977698640.608371806304931886",
        "id": 3
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1786807407.603672,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_ga",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "GA1.1.780964412.1751977698",
        "id": 4
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1786807407.601898,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_ga_C687J1NLT6",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "GS2.1.s1752247404$o4$g1$t1752247407$j57$l0$h0",
        "id": 5
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1786789174.973726,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_ga_NLN9NLDTRF",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "GS2.1.s1752225919$o2$g1$t1752229174$j16$l0$h1521059545",
        "id": 6
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1786807405.38757,
        "hostOnly": False,
        "httpOnly": False,
        "name": "_ga_TRNTF04CYF",
        "path": "/",
        "sameSite": "unspecified",
        "secure": False,
        "session": False,
        "storeId": "0",
        "value": "GS2.1.s1752247405$o4$g0$t1752247405$j60$l0$h0",
        "id": 7
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1759753698,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_gcl_au",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "1.1.929399312.1751977698.1654532706.1752225922.1752229096",
        "id": 8
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1760005159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_gcl_aw",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "GCL.1752229159.CjwKCAjw7MLDBhAuEiwAIeXGIXFD4nVJHs9Enw7Aha9j-G2N79VBQNrmr6BIG6rSJjLtNj1JXkScxxoClckQAvD_BwE",
        "id": 9
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1760005159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_gcl_gs",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "2.1.k1$i1752229157$u214495242",
        "id": 10
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1785673696,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_lb",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "7524686908229305000",
        "id": 11
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1785673696,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_lb_ccc",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "1",
        "id": 12
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1783762072,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_omra",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": true,
        "session": false,
        "storeId": "0",
        "value": "%7B%22ifwhmdkavs5goqbs8xyu%22%3A%22click%22%7D",
        "id": 13
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1760005159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_tt_enable_cookie",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "1",
        "id": 14
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1760005159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "_ttp",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "01JZN049FEQQC5FQ56A1FDN987_.tt.1",
        "id": 15
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1786393159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "cto_bundle",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "O7PkE19lb1QlMkJOS0NoU0w4T1hvTkZrendMOXgxN1czVGhRd1RUVDZxdU5iSzlWSHElMkY2Q3d6Yk9vaiUyRkY0SDd5YlZXT2UlMkZxJTJGNCUyRm53S2NVMXlKcGNTWW83Rm01Y1o2OUF3TkZIVnBUWmVtend3YTNjNkg5ZTdRYUhUVzlWbzNwR0p5dXRwcXVoUGZYMVVWMkY0JTJCam13ZlUlMkZLZCUyRlElM0QlM0Q",
        "id": 16
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1786440324.53691,
        "hostOnly": false,
        "httpOnly": false,
        "name": "ELOQUA",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "GUID=1ED59985BEAF484FA7D56D2C383CA32F",
        "id": 17
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1767529698,
        "hostOnly": false,
        "httpOnly": false,
        "name": "OptanonAlertBoxClosed",
        "path": "/",
        "sameSite": "lax",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "2025-07-08T12:28:18.202Z",
        "id": 18
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1767781159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "OptanonConsent",
        "path": "/",
        "sameSite": "lax",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "isGpcEnabled=0&datestamp=Fri+Jul+11+2025+12%3A19%3A19+GMT%2B0200+(czas+%C5%9Brodkowoeuropejski+letni)&version=202401.2.0&browserGpcFlag=0&isIABGlobal=false&consentId=b5b9a861-5e9b-4b5a-bd69-08cec2034e8e&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&hosts=H309%3A1%2CH40%3A1%2CH730%3A1%2CH1085%3A1%2CH709%3A1%2CH1%3A1%2CH2%3A1%2CH118%3A1%2CH5%3A1%2CH865%3A1%2CH122%3A1%2CH7%3A1%2CH546%3A1%2CH1050%3A1%2CH125%3A1%2CH1227%3A1%2CH1089%3A1%2CH18%3A1%2CH128%3A1%2CH1215%3A1%2CH1060%3A1%2CH248%3A1%2CH1095%3A1%2CH28%3A1&genVendors=&geolocation=PL%3B14&AwaitingReconsent=false",
        "id": 19
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1760005159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "ttcsid",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "1752225920207::jxV9cjZRBTnbCon3mTnc.2.1752229159748",
        "id": 20
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1760005159,
        "hostOnly": false,
        "httpOnly": false,
        "name": "ttcsid_CO77DQBC77U4V9CKI4J0",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "1752225920207::9Qac2pOwF4XqFzv83QuX.2.1752229159994",
        "id": 21
    },
    {
        "domain": ".lex.pl",
        "expirationDate": 1752249209,
        "hostOnly": false,
        "httpOnly": false,
        "name": "userlane-app-session-ol9z9",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": true,
        "session": false,
        "storeId": "0",
        "value": "eyJ2YWx1ZSI6IntcImZvcmVpZ25Vc2VySWRcIjoxMzMxMjM3MDcsXCJzZXNzaW9uSWRcIjpcIjE3ZDIyYmNjLTAwN2MtNDE3NC04OWFiLWEzNzNjYWNiOGJlMlwiLFwiY3JlYXRlZEF0XCI6MTc1MjI0NTIxMDgwOCxcInVwZGF0ZWRBdFwiOjE3NTIyNDc0MDk1MDl9In0=",
        "id": 22
    },
    {
        "domain": ".sip.lex.pl",
        "hostOnly": false,
        "httpOnly": false,
        "name": "__utmzzses",
        "path": "/",
        "sameSite": "unspecified",
        "secure": false,
        "session": true,
        "storeId": "0",
        "value": "1",
        "id": 23
    },
    {
        "domain": ".sip.lex.pl",
        "expirationDate": 1767799405,
        "hostOnly": false,
        "httpOnly": false,
        "name": "OptanonConsent",
        "path": "/",
        "sameSite": "lax",
        "secure": false,
        "session": false,
        "storeId": "0",
        "value": "isGpcEnabled=0&datestamp=Fri+Jul+11+2025+17%3A23%3A25+GMT%2B0200+(czas+%C5%9Brodkowoeuropejski+letni)&version=202503.2.0&browserGpcFlag=0&isIABGlobal=false&consentId=b5b9a861-5e9b-4b5a-bd69-08cec2034e8e&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&hosts=H309%3A1%2CH40%3A1%2CH730%3A1%2CH1085%3A1%2CH709%3A1%2CH1%3A1%2CH2%3A1%2CH118%3A1%2CH5%3A1%2CH865%3A1%2CH122%3A1%2CH7%3A1%2CH546%3A1%2CH1050%3A1%2CH125%3A1%2CH1227%3A1%2CH1089%3A1%2CH18%3A1%2CH128%3A1%2CH1215%3A1%2CH1060%3A1%2CH248%3A1%2CH1095%3A1%2CH28%3A1&genVendors=&geolocation=PL%3B14&AwaitingReconsent=false",
        "id": 24
    },
    {
        "domain": "sip.lex.pl",
        "hostOnly": true,
        "httpOnly": true,
        "name": "__Host-bsid",
        "path": "/",
        "sameSite": "lax",
        "secure": true,
        "session": true,
        "storeId": "0",
        "value": "dDlYSzlEUjBDcXhybGpoS3NlVkdiSVoxUlpVQw==",
        "id": 25
    },
    {
        "domain": "sip.lex.pl",
        "hostOnly": true,
        "httpOnly": false,
        "name": "app_server_lang",
        "path": "/",
        "sameSite": "no_restriction",
        "secure": true,
        "session": true,
        "storeId": "0",
        "value": "PL",
        "id": 26
    },
    {
        "domain": "sip.lex.pl",
        "hostOnly": true,
        "httpOnly": true,
        "name": "JSESSIONID",
        "path": "/",
        "sameSite": "lax",
        "secure": true,
        "session": true,
        "storeId": "0",
        "value": "node01ahceqgzep25l115ao1c1dq44t123722.node0",
        "id": 27
    },
    {
        "domain": "sip.lex.pl",
        "hostOnly": true,
        "httpOnly": false,
        "name": "newSession",
        "path": "/",
        "sameSite": "lax",
        "secure": false,
        "session": true,
        "storeId": "0",
        "value": "false",
        "id": 28
    },
    {
        "domain": "sip.lex.pl",
        "hostOnly": true,
        "httpOnly": false,
        "name": "XSRF-TOKEN",
        "path": "/",
        "sameSite": "unspecified",
        "secure": true,
        "session": true,
        "storeId": "0",
        "value": "52f068e0-4084-4655-9966-d87f8eb21cbd",
        "id": 29
    }
]



options = webdriver.ChromeOptions()

options.add_argument(r"--user-data-dir=C:\Users\karol\AppData\Local\Google\Chrome\'User Data'")

options.add_argument("--profile-directory='Profile 2'")
# options.add_argument("--user-data-dir=/mnt/c/Users/karol/AppData/Local/Google/Chrome/User Data")
# options.add_argument("--profile-directory=Profile 2")

options.add_argument('--start-maximized')

service = Service(executable_path=r"C:\Users\karol\miniconda3\envs\scraper_conda\Scripts\chromedriver.exe")

# options.add_argument("--headless")

driver = webdriver.Chrome(service=service, options=options)


driver.get("https://borg.wolterskluwer.pl")




# WebDriverWait(driver, 10).until(
#     EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
# ).click()
# driver.find_element(By.ID, "onetrust-accept-btn-handler").click()
if driver.find_elements(By.ID, "login"):
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "login"))
    ).send_keys("xolar1989@o2.pl")

    driver.find_element(By.ID, "password").send_keys("Awglm12345!")

    driver.find_element(By.ID, "remember_btn").click()
    driver.find_element(By.ID, "login_btn").click()



# element.click()

time.sleep(5)

# Now you should be authenticated and redirected to sip.lex.pl

r = driver.find_element(By.CSS_SELECTOR, '.product_item_container')


r.click()
# driver.get("https://sip.lex.pl")

time.sleep(5)


original_window = driver.current_window_handle

# Wait for the new window
WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))

# Switch to the new window
for handle in driver.window_handles:
    if handle != original_window:
        driver.switch_to.window(handle)
        break

# driver.find_element(By.CSS_SELECTOR, ".wk-button-success.wk-brg-btn-size").click()
if driver.find_elements(By.ID, "onetrust-accept-btn-handler"):
    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
    ).click()

# driver.find_element(By.ID, "onetrust-accept-btn-handler").click()

# driver.find_element(By.CSS_SELECTOR, "button.left-menu-button[data-gtm='left-menu-HAMBURGER']").click()

if driver.find_elements(By.XPATH, '//*[@id="lex"]/div/header/ng-include/div/nav/div/div[1]/app-left-menu-button/div/button'):
    element = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="lex"]/div/header/ng-include/div/nav/div/div[1]/app-left-menu-button/div/button'))
    )
    element.click()


# WebDriverWait(driver, 10).until(
#     EC.element_to_be_clickable((By.CSS_SELECTOR, "#lex > div > header > ng-include > div > nav > div > div.top-header-left.ng-scope > app-left-menu-button > div > button"))
# ).click()
# #lex > div > header > ng-include > div > nav > div > div.top-header-left.ng-scope > app-left-menu-button > div > button

html = driver.page_source
if "Zasoby" in html:
    print("Zasoby is on the page.")
else:
    print("Zasoby NOT found.")
#
# WebDriverWait(driver, 10).until(
#     EC.element_to_be_clickable((By.XPATH, "//a[span[text()='Zasoby']]"))
# ).click()
# WebDriverWait(driver, 10).until(
#     EC.element_to_be_clickable((By.XPATH, "//a[contains(., 'Zasoby')]"))
# ).click()

# https://sip.lex.pl/#/search-resources?p=%7B%22documentMainType%22:%22QA%22,%22domains%22:%5B%7B%22label%22:%22Podatki%22,%22conceptId%22:4697%7D%5D,%22qaValidity%22:%7B%22nrs%22:134217729,%22label%22:%22aktualne%22%7D%7D&sortBy=default
driver.get("https://sip.lex.pl/#/search-resources?p=%7B%22documentMainType%22:%22QA%22,%22domains%22:%5B%7B%22label%22:%22Podatki%22,%22conceptId%22:4697%7D%5D,%22qaValidity%22:%7B%22nrs%22:134217729,%22label%22:%22aktualne%22%7D%7D&sortBy=default")

r =4

matching_requests = [
    request for request in driver.requests
    if request.response and "searchResults.get.json" in request.url
]

for request in matching_requests:
    try:
        body = request.response.body.decode('utf-8')
        data = json.loads(body)
        print(json.dumps(data, indent=2))  # lub zrób coś z danymi
    except Exception as e:
        print("❌ Błąd dekodowania:", e)

print(f"Captured {len(matching_requests)} matching requests")
#
# element = WebDriverWait(driver, 10).until(
#     EC.element_to_be_clickable((
#         By.XPATH,
#         '//*[@id="narrowings"]/ng-include[1]/div/ul/li[2]/a'
#     ))
# )

# /html/body/div[1]/div/header/ng-include/div/nav/div/div[1]/app-left-menu-button/div/button

#lex > div > header > ng-include > div > nav > div > div.top-header-left.ng-scope > app-left-menu-nav > div > nav > ul > li:nth-child(4) > a
# element.click()

# resources_link = driver.find_element(By.CSS_SELECTOR, "a[data-gtm='left-menu-RESOURCES']")
# resources_link.click()


session_storage_data = {
    "userlane-session": "ab1b0qkamcyz6iiv",
    "userlane-tab-timestamp": "1752249157051"
}

for key, value in session_storage_data.items():
    driver.execute_script(f"window.sessionStorage.setItem('{key}', '{value}');")


driver.get("https://sip.lex.pl/protected-page")

time.sleep(2)  # Let it load

# Step 2: Add cookies to the browser
for cookie in cookie_list:
    # Convert fields if necessary
    cookie_clean = {
        "name": cookie["name"],
        "value": cookie["value"],
        "path": cookie.get("path", "/"),
        "secure": cookie.get("secure", False),
        "httpOnly": cookie.get("httpOnly", False),
    }


    try:
        driver.add_cookie(cookie_clean)
    except Exception as e:
        print(f"Failed to add cookie {cookie['name']}: {e}")


driver.refresh()

driver.get("https://sip.lex.pl/#/search-resources")


w =4
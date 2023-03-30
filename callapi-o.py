#pip install -U python-dotenv
import apifacebook
import os
from dotenv import load_dotenv
#load_dotenv()

# OR, the same with increased verbosity
load_dotenv(verbose=True)

# OR, explicitly providing path to '.env'
from pathlib import Path  # Python 3.6+ only
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

TOKEN_COMEX = os.getenv("TOKEN_COMEX")
TOKEN_SACATUCUENTA =  os.getenv("TOKEN_SACATUCUENTA")
TOKEN_PIENSAPE =  os.getenv("TOKEN_PIENSAPE")

print(TOKEN_COMEX)
print(TOKEN_SACATUCUENTA)
print(TOKEN_PIENSAPE)

Clientes = [
			{'Cliente':'Comex','access_token':TOKEN_COMEX,'cuenta':'141796229342716'},
            {'Cliente':'Saca tu cuenta', 'access_token':TOKEN_SACATUCUENTA,'cuenta':'100330785132565'},
            {'Cliente':'Piensape','access_token':TOKEN_PIENSAPE,'cuenta':'101816778261420'}
            ]

for i in Clientes:
	print(i['Cliente'])
	print(i['access_token'])
	print(i['cuenta'])
	calldata = apifacebook.ApiFacebook(i['Cliente'], i['access_token'],i['cuenta'])
	calldata.get_procesar()

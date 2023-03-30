instalar 
pip3 install virtualenv
virtualenv -p python3 venv
cd venv/Scripts/activate (activar el virtualenv)
pip install -r requirements.txt
pip3 install -U textblob

opcional 

python -m textblob.download_corpora

comando para ejecutar

streamlit run callapi.py

python Sentimient_TextBlob.py
import os
import requests
import logging
import datetime as dt
import zipfile 
import io
import time
import shutil
import random 

# Basic logger setting 
logging.basicConfig(
    filename = 'pipeline.log',
    level = logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s' 
)

def download_file(url, data_path, file_name):
    # Create the folder data/bronze to store the raw files
    os.makedirs(os.path.join(data_path, 'bronze'), exist_ok = True)
    # Get the download date
    download_date = dt.datetime.now().strftime('%Y%m%d')
    # Create the url
    file_path = os.path.join(data_path, 'bronze', f'{file_name}_{download_date}.csv')
    # Log the start of the download
    logging.info(f'Start download from: {url}')
    # Make a mask to GET request 
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 OPR/129.0.0.0'
    }
    # Stronger header
    header_s = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
    }
    # Send HTTP Get request with stream to only open the source
    for attempt in range(1,4):
        try:
            with requests.get(url, stream=True, headers=header_s) as response:
                response.raise_for_status() # Check for HTTP errors
            if response.status_code == 200:
                if '.zip' in url:
                    fake_zip = io.BytesIO()
                    for chunk in response.iter_content(chunk_size=8192):
                        fake_zip.write(chunk)
                    fake_zip.seek(0)
                    # Review if the file is empty
                    if len(fake_zip.getvalue()) < 22: # 22 bytes is the minimun for a zip file
                        logging.warning(f'Server sent an empty file to {file_name}. Skip...')
                        return
                    with zipfile.ZipFile(fake_zip) as mi_zip:
                        for file in mi_zip.namelist():
                            if (file.endswith('.csv') or file.endswith('.txt')) and ('diccionario' not in file.lower()) and ('defunciones' in file.lower() or 'egresos' in file.lower()):
                                with mi_zip.open(file) as intern_file:
                                    with open(file_path, 'wb') as output_file:
                                        shutil.copyfileobj(intern_file, output_file) # Shutil make chunks automatically
                                        # Message of success
                                        logging.info(f'Download success, file saved in: {file_path}')
                                        return
                else:
                    with open(file_path, 'wb') as file:
                        for chunk in response.iter_content(chunk_size=8192): # Download the file in chunks (8 kb)
                            if chunk:
                                file.write(chunk) # response.content chunk contains the downloaded piece of the file
                    # Message of success
                    logging.info(f'Download success, file saved in: {file_path}')
                    return
            else:
                logging.error(f'Download failure. Status code: {response.status_code}')
        except Exception as e:
            logging.critical(f"Error in request: {e}\nTry: {attempt}")
            time.sleep(5)
            
    
# Dictionary with the download links:
data_sources = {
    'poblacion_conapo': 'https://www.datos.gob.mx/dataset/f2b9b220-3ef7-4e3a-bde6-87e1dac78c6a/resource/3c3092be-583e-4490-8c23-67ef9a64b198/download/pobproy_quinq1.csv',
    'mortalidad_inegi_2024': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2024/conjunto_de_datos_edr2024_csv.zip',
    'mortalidad_inegi_2023': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2023/conjunto_de_datos_defunciones_registradas_2023_csv.zip',
    'mortalidad_inegi_2022': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2022/conjunto_de_datos_defunciones_registradas_2022_csv.zip',
    'mortalidad_inegi_2021': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2021/conjunto_de_datos_defunciones_registradas_2021_csv.zip',
    'mortalidad_inegi_2020': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2020/conjunto_de_datos_defunciones_registradas_2020_csv.zip',
    'mortalidad_inegi_2019': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2019/conjunto_de_datos_defunciones_registradas_2019_csv.zip',
    'mortalidad_inegi_2018': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2018/conjunto_de_datos_defunciones_registradas_2018_csv.zip',
    'mortalidad_inegi_2017': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2017/conjunto_de_datos_defunciones_generales_2017_csv.zip',
    'mortalidad_inegi_2016': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2016/defunciones_base_datos_2016_csv.zip',
    'mortalidad_inegi_2015': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2015/defunciones_base_datos_2015_csv.zip',
    'mortalidad_inegi_2014': 'https://www.inegi.org.mx/contenidos/programas/edr/datosabiertos/defunciones/2014/defunciones_base_datos_2014_csv.zip',
    'egresos_dgis_2018': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2018.zip?v=1.1',
    'egresos_dgis_2017': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2017.zip?v=1.1',
    'egresos_dgis_2016': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2016.zip?v=1.1',
    'egresos_dgis_2025': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2025.zip?v=2026.03.20',
    'egresos_dgis_2024': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2024.zip?v=2025.05.22',
    'egresos_dgis_2023': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2023.zip?v=2024.05.31',
    'egresos_dgis_2022': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2022.zip?v=2023.05.22',
    'egresos_dgis_2021': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2021.zip?v=2022.06.15',
    'egresos_dgis_2020': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2020.zip?v=2022.03.17',
    'egresos_dgis_2019': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2019.zip?v=1.1',
    'egresos_dgis_2015': 'http://www.dgis.salud.gob.mx/descargas/datosabiertos/egresos/ssa_egresos_2015.zip?v=1.1'
}

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_PATH, '..', 'data') 

if __name__=="__main__":
    for source, uri in data_sources.items():
        download_file(uri, DATA_PATH, source)
        
        # Stop for human behavior
        pause = random.randint(8,15)
        logging.info(f'Stop time {pause} to evade blockades')
        time.sleep(pause)
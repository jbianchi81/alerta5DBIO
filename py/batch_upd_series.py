import pandas as pd
import subprocess
data = pd.read_csv('/home/alerta7/SSTD-CIC-SA/FEWS-CdP/trunk/SSTD-CdP/SSTD-CdP/Config/MapLayerFiles/P.csv')
feature_ids = data[data["IMPORT_SOURCE"] == "WHOS"]["EXTERNAL_LOCATION_ID"]
for id in feature_ids:
    print(id)
    subprocess.run(["node", "crud_procedures","get-series","whos_om_ogc_timeseries_api","id_externo=%s" % id, "-u"], check=True)
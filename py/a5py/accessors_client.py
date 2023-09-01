import requests
#import pydrodelta.util as util
import json

config_file = open("config.json")
config = json.load(config_file)
config_file.close()

def readAccessor(accessor_id,use_proxy=False):
    response = requests.get("%s/accessors/%s" % (config["api"]["url"], accessor_id),
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def readObservaciones(accessor_id,estacion_id=None,var_id=None,series_id=None,timestart=None,timeend=None,use_proxy=False,tipo=None):
    params = {
        "estacion_id": estacion_id,
        "var_id": var_id,
        "series_id": series_id,
        "timestart": timestart if timestart is not None and isinstance(timestart,str) else timestart.isoformat(),
        "timeend": timeend if timeend is not None and isinstance(timeend,str) else timeend.isoformat(),
        "tipo": tipo
    }
    response = requests.get("%s/accessors/%s/get" % (config["api"]["url"], accessor_id),
        params = params,
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def createObservaciones(accessor_id,estacion_id=None,var_id=None,series_id=None,timestart=None,timeend=None,use_proxy=False,tipo=None):
    params = {
        "estacion_id": estacion_id,
        "var_id": var_id,
        "series_id": series_id,
        "timestart": timestart if timestart is not None and isinstance(timestart,str) else timestart.isoformat(),
        "timeend": timeend if timeend is not None and isinstance(timeend,str) else timeend.isoformat(),
        "tipo": tipo
    }
    response = requests.post("%s/accessors/%s/update" % (config["api"]["url"], accessor_id),
        json = params,
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response


def readEstaciones(accessor_id,estacion_id=None,return_raw=False,use_proxy=False):
    params = {
        "estacion_id": estacion_id,
        "return_raw": return_raw
    }
    response = requests.get("%s/accessors/%s/getSites" % (config["api"]["url"], accessor_id),
        params = params,
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def readSeries(accessor_id,estacion_id=None,var_id=None,series_id=None,return_raw=False,use_proxy=False,tabla_id=None,fuentes_id=None,area_id=None,escena_id=None,include_geom=None,id_externo=None,proc_id=None,unit_id=None,tipo="puntual",timestart=None,timeend=None):
    params = {
        "estacion_id": estacion_id,
        "var_id": var_id,
        "series_id": series_id,
        "return_raw": return_raw,
        "tabla_id": tabla_id,
        "fuentes_id": fuentes_id,
        "area_id": area_id,
        "escena_id": escena_id,
        "include_geom": include_geom,
        "id_externo": id_externo,
        "proc_id": proc_id,
        "unit_id": unit_id,
        "tipo": tipo,
        "timestart": timestart,
        "timeend": timeend
    }
    response = requests.get("%s/accessors/%s/getSeries" % (config["api"]["url"], accessor_id),
        params = params,
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    if response.status_code != 200:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def createEstaciones(accessor_id,use_proxy=False):
    response = requests.post("%s/accessors/%s/updateSites" % (config["api"]["url"], accessor_id),
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    print("status code: %i" % response.status_code)
    if response.status_code >= 400:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def createSeries(accessor_id,use_proxy=False):
    response = requests.post("%s/accessors/%s/updateSeries" % (config["api"]["url"], accessor_id),
        headers = {'Authorization': 'Bearer ' + config["api"]["token"]},
        proxies = config["proxy_dict"] if use_proxy else None
    )
    print("status code: %i" % response.status_code)
    if response.status_code >= 400:
        raise Exception("request failed: %s" % response.text)
    json_response = response.json()
    return json_response

def updateSeries(accessor_id,timestart,timeend,estacion_id=None,var_id=None,series_id=None,return_raw=False,use_proxy=False,tabla_id=None,fuentes_id=None,area_id=None,escena_id=None,include_geom=None,id_externo=None,proc_id=None,unit_id=None,tipo="puntual"):
    """
    get series of accessor (optionally filtered) and update given timestart and timeend
    """
    series = readSeries(accessor_id,estacion_id=estacion_id,var_id=var_id,series_id=series_id,return_raw=return_raw,use_proxy=use_proxy,tabla_id=tabla_id,fuentes_id=fuentes_id,area_id=area_id,escena_id=escena_id,include_geom=include_geom,id_externo=id_externo,proc_id=proc_id,unit_id=unit_id,tipo=tipo,timestart=timeend,timeend=timestart)
    print("Got %i series" % len(series))
    for serie in series:
        print("updating serie %i" % serie["id"])
        try:
            createObservaciones(accessor_id,series_id=serie["id"],tipo=serie["tipo"],timestart=timestart,timeend=timeend)
        except Exception:
            print("Failed to update serie: %s" % str(Exception))



# QMD
node crud_procedures.js get-sites whos_om_ogc_timeseries_api country=URY provider=uruguay-dinagua observedProperty=discharge timeInterpolation=AVERAGE aggregationDuration=P1D beginPosition=2025-01-01 endPosition=2025-07-23 ontology=whos -o tmp/sites_dinagua_q.json -p -u
node crud_procedures.js get-series whos_om_ogc_timeseries_api country=URY provider=uruguay-dinagua observedProperty=discharge timeInterpolation=AVERAGE aggregationDuration=P1D beginPosition=2025-01-01 endPosition=2025-07-23 ontology=whos -o tmp/series_dinagua_q.json -p -u
# mapear unidades y variables y repetir
node crud_procedures.js get-series whos_om_ogc_timeseries_api country=URY provider=uruguay-dinagua observedProperty=discharge timeInterpolation=AVERAGE aggregationDuration=P1D beginPosition=2025-01-01 endPosition=2025-07-23 ontology=whos -o tmp/series_dinagua_q.json -p -u
node crud_procedures.js get whos_om_ogc_timeseries_api provider=uruguay-dinagua var_id=40 timestart=2025-07-01 timeend=2025-07-25 -o tmp/data_dinagua.json -p -u

# HMD
node crud_procedures.js get-sites whos_om_ogc_timeseries_api country=URY provider=uruguay-dinagua observedProperty=level timeInterpolation=AVERAGE aggregationDuration=P1D beginPosition=2025-01-01 endPosition=2025-07-23 ontology=whos -o tmp/sites_dinagua_h.json -p -u
node crud_procedures.js get-series whos_om_ogc_timeseries_api country=URY provider=uruguay-dinagua observedProperty=level timeInterpolation=AVERAGE aggregationDuration=P1D beginPosition=2025-01-01 endPosition=2025-07-23 ontology=whos -o tmp/series_dinagua_h.json -p
# mapear unidades y variables
node crud_procedures.js get-series whos_om_ogc_timeseries_api country=URY provider=uruguay-dinagua observedProperty=level timeInterpolation=AVERAGE aggregationDuration=P1D beginPosition=2025-01-01 endPosition=2025-07-23 ontology=whos -o tmp/series_dinagua_q.json -p -u
node crud_procedures.js get whos_om_ogc_timeseries_api provider=uruguay-dinagua var_id=39 timestart=2025-07-01 timeend=2025-07-25 -o tmp/data_dinagua.json -p -u
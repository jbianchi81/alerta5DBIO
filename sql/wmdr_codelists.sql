drop table if exists "wmdr_ObservedVariable";
create table "wmdr_ObservedVariable" (
    id varchar primary key,
    description varchar,
    label varchar not null,
    notation varchar);


\COPY "wmdr_ObservedVariable" (id,description,label,notation) FROM 'data/wmdr/ObservedVariableTerrestrial.csv' with csv;
\COPY "wmdr_ObservedVariable" (id,description,label,notation) FROM 'data/wmdr/ObservedVariableAtmosphere.csv' with csv;

drop table if exists "VariableName";
create table "VariableName" (
    "VariableName" varchar primary key,
    wmdr_id varchar
);

INSERT INTO "VariableName" ("VariableName",wmdr_id) VALUES
('antecedent precipitation index', NULL),
('Barometric pressure','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/216'),
('Cloud cover','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/180'),
('Discharge','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/171'),
('Evapotranspiration','http://codes.wmo.int/bufr4/b/13/031'),
('flood guidance','http://codes.wmo.int/grib2/codeflag/4.2/_1-0-0'),
('Flood magnitude',NULL),
('Gage height','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12252'),
('Global Radiation','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/573'),
('Precipitation','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/210'),
('Relative humidity','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12249'),
('Reservoir inflow',NULL),
('Reservoir outflow',NULL),
('Reservoir spilled',NULL),
('Reservoir storage','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12279'),
('Sea-level pressure','http://codes.wmo.int/bufr4/b/10/051'),
('Snow depth','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/629'),
('Sunshine duration','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/270'),
('Temperature','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12166'),
('Temperature, dew point','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/225'),
('Tidal stage','http://codes.wmo.int/bufr4/b/22/038'),
('Transfered discharge',NULL),
('Visibility','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/230'),
('Volumetric water content','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12277'),
('Water extent','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/243'),
('Water level','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/12252'),
('Wind direction','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12005'),
('Wind Direction','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12005'),
('Wind speed','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12006'),
('Wind Speed','http://codes.wmo.int/wmdr/ObservedVariableAtmosphere/12006'),
('Wet-bulb temperature','http://codes.wmo.int/bufr4/b/12/002'),
('Evaporation','http://codes.wmo.int/wmdr/ObservedVariableTerrestrial/510'),
('u-component of wind','http://codes.wmo.int/grib2/codeflag/4.2/0-2-2'),
('v-component of wind','http://codes.wmo.int/grib2/codeflag/4.2/0-2-3');
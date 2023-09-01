from  datetime import datetime, timedelta 
import pandas

def batchUpdateFromAccessor(accessor_id,datasets_filepath):
    datasets = pandas.read_csv(datasets_filepath)
    for i, dataset in datasets.iterrows():
        tabla_id = dataset[0]
        estacion_id = None if dataset[1] == "*" else int(dataset[1])
        var_id = None if dataset[2] == "*" else int(dataset[2])
        proc_id = None if dataset[3] == "*" else int(dataset[3])
        timestart = datetime.now() - timedelta(days=dataset[4])
        timeend = datetime.now()
        print((tabla_id, estacion_id, var_id, proc_id, str(timestart)) )
        client.updateSeries(accessor_id,tabla_id=tabla_id,var_id=var_id,proc_id=proc_id,timestart=timestart,timeend=timeend)

if __name__ == "__main__":
    datasets_filepath =  "../config/datasets2.csv"
    accessor_id = "a5"
    batchUpdateFromAccessor(accessor_id,datasets_filepath)
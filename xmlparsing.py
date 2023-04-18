import xml.etree.cElementTree as et
import pandas as pd
import pymongo as pm
import json
import os

def insert_data_to_db(df):

    user = "mmayanna"
    password = "UkDfEw6v0fLZQCNA"

    clientString = [
        'mongodb+srv://' + user + ':' + password + '@clustersaptest.i1ivo.mongodb.net/myFirstDatabase?retryWrites=true&w=majority']
    client = pm.MongoClient(clientString, ssl=True)
    mydb = client["BathametricsData"]
    collection = mydb['noaa-bathymetry']
    data = df.to_dict(orient='records')
    collection.insert_many(data)
    print('done')
    client.close()

def parseXML(xmlfile, df):
    # create element tree object
    tree = et.parse(xmlfile)

    # get root element
    root = tree.getroot()

    lst = []
    for item in root.iter('Row'):
        ll = []
        for i in item:
            ll.append(i.text)
        lst.append(ll)

    for i in lst:
        df.loc[len(df)] = i

    return df




def get_file_names(folder_path):

    files_list = []

    for file_name in os.listdir(folder_path):
        # Check if the file is a regular file (not a directory)
        if os.path.isfile(os.path.join(folder_path, file_name)):
            # Append the file name to the list
            files_list.append(file_name)
    return files_list


def main():

    #Download the xml files into below folder which needs to be parsed
    folder_path = "./xml_files"
    files_list = get_file_names(folder_path)
    df = pd.DataFrame(columns=['value', 'count', 'data_assessment', 'feature_least_depth', 'significant_features', 'feature_size', 'coverage', 'bathy_coverage', 'horizontal_uncert_fixed', 'horizontal_uncert_var', 'vertical_uncert_fixed', 'vertical_uncert_var', 'license_name','license_url', 'source_survey_id', 'source_institution', 'survey_date_start', 'survey_date_end'])

    for file in files_list:
        df = parseXML(folder_path+'/'+file, df)

    insert_data_to_db(df)


if __name__ == "__main__":
    # calling main function
    main()

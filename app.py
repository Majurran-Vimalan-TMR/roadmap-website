from flask import Flask
from flask import Blueprint, render_template, request, flash, jsonify, redirect, url_for, current_app
import plotly
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
from ast import literal_eval
app = Flask(__name__)



@app.route('/')
def hello_world():
    adf_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/ROAR to AMMS ADF/AMMS_BASE_ITSE_202307282235.ADF/AMMS ITSE ADF Proper Columns.csv"


    def dataframe_length(file_path):
        try:
            dataframe_1 = pd.read_csv(file_path)
        except Exception:
            dataframe_1 = pd.read_excel(file_path)
        return len(dataframe_1)

    sites = True
    if sites:
        frontend_column = "SITE_ID"
        database_column = "SITE_ID"
        amms_column = "Code"

        frontend_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/ROAR Front End/Sites/site_adf-2023-07-29T03_17_13.645Z.xlsx"
        database_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/TSDM Database/AR_SITES_ALL.xlsx"
        amms_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/AMMS DB/ElectricalSite.csv"
        sites_database_len = dataframe_length(database_path)
        sites_amms_len = dataframe_length(amms_path)
        sites = not sites
    if not sites:
        frontend_column = "ASSET_ID"
        database_column = "ASSET_ID"
        amms_column = "Code"
        frontend_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/ROAR Front End/Assets/Full ROAR Frontend Asset List.csv"
        database_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/TSDM Database/AR_ASSETS_ALL.csv"
        amms_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/AMMS DB/ElectricalAsset.csv"
        amms_asset_path = "C:/Users/mzvimal/TMR/RoadMap - Documents/General/03 Project Working Folder/05 Reference Documentation/05 ITSE/20230729/AMMS DB/Assets.csv"
        asset_database_len = dataframe_length(database_path)
        asset_amms_len = dataframe_length(amms_path)
    adf_column = "ID"


    def merge(file_path_1, file_path_2, dataframe_1_column,dataframe_2_column,detail_string,dataframe_1_filters=[],
                    dataframe_2_filters = []):
        try:
            dataframe_1 = pd.read_csv(file_path_1)
        except Exception:
            dataframe_1 = pd.read_excel(file_path_1)

        try:
            dataframe_2 = pd.read_csv(file_path_2)
        except Exception:
            dataframe_2 = pd.read_excel(file_path_2)

    ##    try:
    ##        dataframe_3 = pd.read_csv(file_path_3)
    ##    except Exception:
    ##        dataframe_3 = pd.read_excel(file_path_3)
        
        print(dataframe_2.columns)

        dataframe_1[dataframe_1_column]
        dataframe_2[dataframe_2_column]
        
        #CUstom filters
        for filter_string in dataframe_1_filters:
            dataframe_1 = eval(filter_string)
        for filter_string in dataframe_2_filters:
            dataframe_2 = eval(filter_string)

        merged_dataframe = dataframe_1.merge(dataframe_2,how="inner",left_on=dataframe_1_column,right_on=dataframe_2_column)
        #merged_dataframe = merged_dataframe.merge(dataframe_3,how="inner",left_on=dataframe_1_column,right_on=dataframe_3_column)

        merged_dataframe.to_csv(detail_string+".csv")
        return merged_dataframe

    invalid_analysis = pd.DataFrame()

    def analyze_row_direct(row,attribute_name,column_1,column_2):
        if row[column_1] != row[column_2]:
            return "The {attribute_name} is {} in the ADF File from ROAR, but the {attribute_name} is {} in AMMS.".format(row[column_1],row[column_2],attribute_name=attribute_name)
    ##        print("The First Dataframe has status {}, but the second dataframe has the status {} for ID, {}"
    ##              .format(row["Status_x"],row["Status_y"],row["Code"]))
        else:
            return None
        

    def analyze_row_extra_attributes(row,attribute_name,extra_attribute,column_2):
        try :
            if str(row[column_2]) == 'nan':
                raise ValueError
            if float(literal_eval(row["ExtraAttributesJson"]).get(extra_attribute,"nan")) != float(row[column_2]):
                return "The {attribute_name} is {} in the ADF File from ROAR, but the {attribute_name} is {} in AMMS.".format(literal_eval(row["ExtraAttributesJson"]).get(extra_attribute,"nan"),str(row[column_2]),attribute_name=attribute_name)
        except ValueError:
            if literal_eval(row["ExtraAttributesJson"]).get(extra_attribute,"nan") != str(row[column_2]):
                return "The {attribute_name} is {} in the ADF File from ROAR, but the {attribute_name} is {} in AMMS.".format(literal_eval(row["ExtraAttributesJson"]).get(extra_attribute,"nan"),str(row[column_2]),attribute_name=attribute_name)


    def consolidate(file_path_1, file_path_2, dataframe_1_column,dataframe_2_column,detail_string,dataframe_1_filters=[],
                dataframe_2_filters = []):
        try:
            dataframe_1 = pd.read_csv(file_path_1)
        except Exception:
            dataframe_1 = pd.read_excel(file_path_1)

        try:
            dataframe_2 = pd.read_csv(file_path_2)
        except Exception:
            dataframe_2 = pd.read_excel(file_path_2)

        
        print(dataframe_2.columns)

        #CUstom filters
        for filter_string in dataframe_1_filters:
            dataframe_1 = eval(filter_string)
        for filter_string in dataframe_2_filters:
            dataframe_2 = eval(filter_string)
        
        dataframe_1_exclusive_asset_count = 0
        dataframe_1_unique_list = []


        dataframe_1_list = set(list(dataframe_1[dataframe_1_column].astype(str)))


        dataframe_2_list = set(list(dataframe_2[dataframe_2_column].astype(str)))

        for index, asset_id in enumerate(dataframe_1_list):
            if asset_id not in dataframe_2_list:
                dataframe_1_unique_list.append(asset_id)
                dataframe_1_exclusive_asset_count += 1

            if index%10000 == 0:
                print( "Up to",index,"/",len(dataframe_1_list))

        return dataframe_1_exclusive_asset_count


    assets_in_roar_not_in_amms = consolidate(database_path, amms_path,database_column,amms_column,"Assets in ROAR Database not in AMMS",
                dataframe_1_filters = [],
                dataframe_2_filters = [])

    assets_in_amms_not_in_roar = consolidate(amms_path,database_path, amms_column,database_column,"Assets in AMMS not in ROAR Database",
                dataframe_1_filters = [],
                dataframe_2_filters = [])
    

    roar_labels = ['Both', 'ROAR Exclusive']
    
    roar_chart = go.Figure(data = [go.Pie(labels = roar_labels, values = [asset_database_len-assets_in_roar_not_in_amms,assets_in_roar_not_in_amms])])
    roar_chart.update_layout(
                width=300,
                height=424,
                title = "Assets Present in Both Databases vs Assets exclusive to ROAR",
    )

    amms_labels = ['Both', 'AMMS Exclusive']
    
    amms_chart = go.Figure(data = [go.Pie(labels = amms_labels, values = [asset_amms_len-assets_in_amms_not_in_roar,assets_in_amms_not_in_roar])])
    amms_chart.update_layout(
                width=300,
                height=424,
                title = "Assets Present in Both Databases vs Assets exclusive to AMMS",
    )
        
##    if sites:
##        pass
##    else:
##        merged_dataframe = merge(amms_path,adf_path, amms_column,adf_column,"Assets in both AMMS and ADF",
##                        dataframe_1_filters = [],
##                        dataframe_2_filters = ['dataframe_2[dataframe_2["Site Type"]=="OAS"]'],)
##        invalid_analysis["ID"] = merged_dataframe["Code"]
##        invalid_analysis["Status"] = merged_dataframe.apply(lambda row: analyze_row_direct(row,"Status","Status_x","Status_y"),axis=1)
##     
##        invalid_analysis["Tariff Rate"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Tariff Rate",extra_attribute="TariffRate",column_2="Tariff Rate"),axis=1)
##
##        invalid_analysis["Cyberlock Installed"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Cyberlock Installed",extra_attribute="CyberlockInstalled",column_2="Cyberlock Installed"),axis=1)
##
##        invalid_analysis["Design Running Load (W)"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Design Running Load (W)",extra_attribute="DesignRunningLoadW",column_2="Design Running Load (W)"),axis=1)
##        
##        invalid_analysis["Operational Status Date"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Operational Status Date",extra_attribute="OperationalStatusDate",column_2="Operational Status Date"),axis=1)
##
##        invalid_analysis["Latitude of Asset"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Latitude of Asset",extra_attribute="LatitudeOfAsset",column_2="Latitude of Asset"),axis=1)
##
##        invalid_analysis["Longitude of Asset"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Longitude of Asset",extra_attribute="LongitudeOfAsset",column_2="Longitude of Asset"),axis=1)
##
##        invalid_analysis["Pole or Post Type"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Pole or Post Type",extra_attribute="PoleOrPostType",column_2="Pole or Post Type"),axis=1)
##
##        invalid_analysis["Cabinet Extension Unit"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Cabinet Extension Unit",extra_attribute="CabinetExtensionUnit",column_2="Cabinet Extension Unit"),axis=1)
##
##        invalid_analysis["Communications Type"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Communications Type",extra_attribute="CommunicationsType",column_2="Communications Type"),axis=1)
##
##        invalid_analysis["Maintaining Authority"] = merged_dataframe.apply(lambda row: analyze_row_extra_attributes(row,attribute_name ="Maintaining Authority",extra_attribute="MaintainingAuthority",column_2="Maintaining Authority"),axis=1)
##
##        invalid_analysis["Manufacturer"] = merged_dataframe.apply(lambda row: analyze_row_direct(row,"Manufacturer","Manufacturer_x","Manufacturer_y"),axis=1)
##
##        invalid_analysis["Model"] = merged_dataframe.apply(lambda row: analyze_row_direct(row,"Model","Model_x","Model_y"),axis=1)

    
        

   
    
    print("Did we reach this point?")
    return render_template('public-dashboard.html',num_roar_assets = asset_database_len,
                           num_amms_assets = asset_amms_len, num_roar_sites = sites_database_len,
                           num_amms_sites = sites_amms_len,roar_chart=roar_chart,amms_chart=amms_chart, graph_activities=amms_chart)
                           

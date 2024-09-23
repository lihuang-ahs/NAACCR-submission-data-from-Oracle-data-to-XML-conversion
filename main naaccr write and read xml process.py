
import oracledb
import csv
import xml.etree.ElementTree as ET

from lxml import etree
import glob
import pandas as pd
import sys
import os
sys.path.append('H:/R_extras/')
sys.path.append('T:\Alberta Cancer Registry\R projects\inhouse general codes\Python')

from configfile import *
from prep_data_connections import *

# Location of xsd and xml dictionary path
xsd_path = 'naaccr_data_1.6.xsd'
dict_path = 'naaccr-dictionary-230.xml'

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Input source data
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

source_data = 'NAACCR_PT_2016'

oracledb.init_oracle_client()
connection = oracledb.connect(user= f"{ORACLE_USERNAME}[ALBERTA_CANCER_REGISTRY_ANLYS]", password=ORACLE_PASSWORD, dsn= ORACLE_TNS_NAME)

qry_cursor = connection.cursor()
qry_cursor.execute(f'SELECT * FROM {source_data}')
rows_fetched  = qry_cursor.fetchall()
cols = [desc[0] for desc in qry_cursor.description]

qry_cursor.close()    
connection.close()

dbf = pd.DataFrame(rows_fetched, columns = cols)

dbf['ind_pt_dup'] = dbf.duplicated(subset = 'patientIdNumber', keep = 'first') # get the duplicated patient a flag 'True'

dbf_nonum = dbf.applymap(str)

dbf_dict = dbf_nonum.to_dict(orient='records')

#-------------------------------------------
# Write functions
#--------------------------------------------  
  
def parse_naaccr_dictionary(dictionary_path):
        # Parse the NAACCR dictionary XML
    tree = etree.parse(dictionary_path)
    root = tree.getroot()
    patient_items = set()
    tumor_items = set()

        # Extract Patient and Tumor item definitions
    for item in root.xpath("//ns:ItemDef", namespaces={'ns': 'http://naaccr.org/naaccrxml'}):
        naaccr_id = item.get("naaccrId")
        parent_tag = item.get("parentXmlElement")
        if parent_tag == "Patient":
            patient_items.add(naaccr_id)
        elif parent_tag == "Tumor":
            tumor_items.add(naaccr_id)
    
    return patient_items, tumor_items


def generate_xml(oracle_data, rec_type):
        # Create the root element of the XML
        root = etree.Element("NaaccrData",
                             xmlns="http://naaccr.org/naaccrxml",
                             baseDictionaryUri="http://naaccr.org/naaccrxml/naaccr-dictionary-230.xml",
                             recordType= rec_type,
                             specificationVersion="1.6")

        # Add global items
        global_items = [
            {"naaccrId": "registryType", "value": "1"},
            {"naaccrId": "recordType", "value": rec_type},
            {"naaccrId": "naaccrRecordVersion", "value": "230"},
            {"naaccrId": "registryId", "value": "0022004800"}
        ]
        for item in global_items:
            item_element = etree.SubElement(root, "Item", naaccrId=item["naaccrId"])
            item_element.text = item["value"]


        # Parse the NAACCR dictionary to get patient and tumor item mappings

    
        patient_items, tumor_items = parse_naaccr_dictionary(dict_path)

        for patient_data in oracle_data:
           
            # Adding Patient Items
            if patient_data['ind_pt_dup'] == "False":
                patient_element = etree.SubElement(root, "Patient")
                for naaccr_id, value in patient_data.items():
                    if naaccr_id in patient_items and value != "None":
                        item_element = etree.SubElement(patient_element, "Item", naaccrId=naaccr_id)
                        item_element.text = value
                        
            # Create a Tumor element
            tumor_element = etree.SubElement(patient_element, "Tumor")
            for naaccr_id, value in patient_data.items():
                
                if naaccr_id in tumor_items and value != "None":
                    item_element = etree.SubElement(tumor_element, "Item", naaccrId=naaccr_id)
                    item_element.text = value

        # Convert the XML tree to a string
        xml_data = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        return xml_data


def save_xml( xml_data, filename):
    output_path = os.path.abspath(filename)
    with open(output_path, 'wb') as file:
        file.write(xml_data)
        

def validate_xml(xml_path, xsd_path):
    # Open and read the XSD file
    with open(xsd_path, 'rb') as xsd_file:
        xsd_content = xsd_file.read()
    # Parse the XSD content
    xsd_doc = etree.XML(xsd_content)
    xsd_schema = etree.XMLSchema(xsd_doc)

    # Open and read the XML file
    with open(xml_path, 'rb') as xml_file:
        xml_content = xml_file.read()
    xml_doc = etree.XML(xml_content)

    # Validate the XML against the schema
    is_valid = xsd_schema.validate(xml_doc)
    
    return is_valid

#-------------------------------------------
# Apply write function and save xml file
#--------------------------------------------   

# Apply functions
oracle_xml = generate_xml(dbf_dict, "I")


save_xml(oracle_xml, f'output/{source_data}.xml')

#--------------------------------------------
# Functions to read in xml into oracle tables
#--------------------------------------------

def parse_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Define the namespaces
    namespaces = {'ns': 'http://naaccr.org/naaccrxml'}

    # Extract the global items
    global_items = {}
    for item in root.findall('ns:Item', namespaces):
        naaccr_id = item.get('naaccrId')
        value = item.text
        global_items[naaccr_id] = value

    # Extract patient and tumor data
    data = []
    for patient in root.findall('ns:Patient', namespaces):
        patient_data = global_items.copy()
        for item in patient.findall('ns:Item', namespaces):
            naaccr_id = item.get('naaccrId')
            value = item.text
            patient_data[naaccr_id] = value

        for tumor in patient.findall('ns:Tumor', namespaces):
            tumor_data = patient_data.copy()
            for item in tumor.findall('ns:Item', namespaces):
                naaccr_id = item.get('naaccrId')
                value = item.text
                tumor_data[naaccr_id] = value
            data.append(tumor_data)

    return data

#--------------------------------------------
# Read function done
#--------------------------------------------


# Apply read function and read xml file

data = parse_xml('output/NAACCR_DATA_2022.xml')
df_data = pd.DataFrame(data).fillna(' ')

df_data.columns = df_data.columns.str.upper()

# --------------------------------------------
# Read into oracle (Only keep vars according to a list of variables
# --------------------------------------------

ref_varname = pd.read_csv('input/read_variables.csv')
ref_varname['NAACCR_ITEM'] = ref_varname['NAACCR_ITEM'].astype(str)

ref_column_list = '","'.join(ref_varname['NAACCR_ITEM'].values)



in_col = [col for col in df_data.columns if col in ref_column_list]
out_col = [col for col in ref_varname['NAACCR_ITEM'].values if col not in df_data.columns]

df_data2 = df_data[in_col]
    
for col in  out_col:
    df_data2[col]   = ' '   

# duplicates = df_data.columns[df_data.columns.duplicated()].tolist()              

data_filled = df_data2.to_dict(orient = 'records')


# Create a empty data structure in oracle with all variables being string variables, and column names in capital letters

oracledb.init_oracle_client()
connection = oracledb.connect(user= f"{ORACLE_USERNAME}[ALBERTA_CANCER_REGISTRY_ANLYS]", password=ORACLE_PASSWORD, dsn= ORACLE_TNS_NAME)


qry_upload2 = 'TRUNCATE TABLE NAACCR_PREP5 DROP ALL STORAGE'

qry_cursor          = connection.cursor()
qry_cursor.execute(qry_upload2) 
connection.commit()
qry_cursor.close()

# Insert data into the table
cursor = connection.cursor()
columns = ', '.join(data_filled[0].keys())
placeholders = ', '.join([f":{key}" for key in data_filled[0].keys()])

insert_sql = f"insert into NAACCR_PREP5 ({columns}) VALUES ({placeholders})"

for row in data_filled:
    cursor.execute(insert_sql, row)
    
# Commit the transaction
connection.commit()
cursor.close()

connection.close()
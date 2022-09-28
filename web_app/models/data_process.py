import csv
import json


def make_json(csvFilePath, jsonFilePath):
	
	data = {}
	
	with open(csvFilePath, encoding='utf-8') as csvf:
		csvReader = csv.DictReader(csvf)
		
		for rows in csvReader:
			
			key = rows['merchant_abn']
			data[key] = rows

	with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
		jsonf.write(json.dumps(data, indent=4))
		

csvFilePath = r'models/merchant.csv'
jsonFilePath = r'models/merchant.json'

# Call the make_json function
make_json(csvFilePath, jsonFilePath)


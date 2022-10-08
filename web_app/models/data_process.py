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
		


top100CSV = r'models/top100.csv'
top100JSON = r'models/top100.json'

healthCSV = r'models/health_top10.csv'
healthJSON = r'models/health_top10.json'

personalCSV = r'models/personal_top10.csv'
personalJSON = r'models/personal_top10.json'

recreationalCSV = r'models/recreational_top10.csv'
recreationalJSON = r'models/recreational_top10.json'

technicalCSV = r'models/technical_top10.csv'
technicalJSON = r'models/technical_top10.json'

# Call the make_json function
make_json(top100CSV, top100JSON)
make_json(healthCSV, healthJSON)
make_json(personalCSV, personalJSON)
make_json(recreationalCSV, recreationalJSON)
make_json(technicalCSV, technicalJSON)


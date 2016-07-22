from dbHandler import add, get
import csv 



with open("LoanStats3c.csv") as data:
	reader = csv.reader(data)
	i = 0
	for row in reader:
		i += 1

		if i <= 1:
			headers = row  
		else:
			doc = {}
			doc['_id'] = str(i)
			for j,value in enumerate(row):
				doc[headers[j]] = value
			add.addJson(doc) 

			# print doc 

		if i % 100 == 0:
			print i 
			# break
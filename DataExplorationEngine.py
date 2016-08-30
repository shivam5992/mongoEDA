from pymongo import MongoClient 
from config import *
import collections
import math
import csv

db = MongoClient(HOST, PORT)[DATABASE_NAME]

class Add:
	''' 
	Function to add a json / dictionary as document into mongo database
	
	params:
	doc - the dictionary to be inserted
	collname - collection name, where to insert the document

	'''
	def add_json(self, doc, collname):
		try:
			db[collname].insert(doc)
		except Exception as E:
			print E

	''' 
	Function to load a csv, dump every row as document in mongo 
	Make sure that a column named "_id" is present in the csv as the unique identifier

	params:
	filename - name of the csv file which contains the data 
	collname - name of the collection where to insert the data 
	'''
	def load_csv(self, filename, collname, datatypes):
		with open(filename) as data:
			reader = csv.reader(data)

			counter = 0
			for row in reader:
				counter += 1
				if counter == 1:
					headers = row  
				else:
					doc = {}
					for j,value in enumerate(row):
						key = headers[j].strip()
						val = value.strip()
						if key in datatypes['floats']:
							doc[key] = float(val)
						else:
							doc[key] = val
					self.add_json(doc, collname) 

				# Print progress
				if counter % 100 == 0:
					print counter 

class Get:
	''' 	
	Funtion to return all documents from a collection

	params:
	collname: name of the collection 
	'''
	def get_documents(self, collname):
		return db[collname].find()

class EDA:
	def __init__(self, missing_types = [None, "NA", "N/A", "Null", "None", ""]):
		self.missing_types = missing_types

	def check_data_type(self, var):
		''' 
		Variable Identification - Data Type 
		Function to detect the variable type
		'''

		if type(var) == float:
			return "Double"
		elif type(var) == int:
			return "Integer"
		elif type(var) == str:
			return "String"
		elif var.isdigit():
			return "Integer"
		elif var.isalnum():
			return "Alphanumeric"
		else:
			return type(var)

	def identify_variable_data_type(self, key):
		distinct = self.getDistinct(key)
		value = distinct[random.randint(0,len(distinct))]
		if value and value not in self.missing_types:
			return self.checkDataType(value)
		return None

	def identify_variable_type(self, key):
		'''
		Variable Identification - Continuous or Categorical
		'''
		
		distinct_count = self.getDistinctCount(key)
		total_count = self.getTotalCount(key)
		ratio_unique = round((float(distinct_count) / total_count) * 100,2)
		if ratio_unique > 0.5:
			return "Categorical"
		else:
			return "Continuous"


	def univariate_analysis(self, key, group_key, collname, limit = False, sorting_order = "DESC", central_tendencies = True):
		''' 
		Variable Analysis - Univariate 

		Function to perform univariate analysis on a variable. Works directly for Categorical Variables. For Continuous
		Variables, one can use binning function first before univariate analysis. 

		params:
		key: Name of the key (variable) 
		collname: Name of the collection which contains the documents
		limit: Number of documents / rows to be analysed, Default is False (all documents)
		sorting_order: Arranging the results in asending or descending order (ASEC or DESC)
		central_tendencies: Boolean, True if you want to include mean, median and mode in the results

		'''

		sorter = -1
		if sorting_order != "DESC":
			sorter = 1

		if central_tendencies:
			pipe = [{'$group' : {'_id' : '$'+key, 'freq' : {'$sum':'$'+group_key}, 'mean':{'$avg':'$'+group_key}, 'min':{'$min':'$'+group_key}, 'max':{'$max':'$'+group_key}}}, 
					{'$sort':{'freq':sorter}}]
		else:
			pipe = [{'$group' : {'_id' : '$'+key, 'freq' : {'$sum':'$'+group_key}}},
					{'$sort':{'freq':sorter}}]

		if limit:
			pipe.append({'$limit':limit})
		res = db[collname].aggregate(pipe)
		res = self.cursor_to_list(res)
		return res 

	def get_distinct(self, key):
		return db[collname].distinct(key)

	def get_distinct_count(self, key):
		return len(self.getDistinct(key))

	def get_total_count(self, key):
		return db[collname].find().count()

	def cursor_to_list(self, cursor):
		return 	[_ for _ in cursor]


	''' Get Missing Count '''
	def getMissingCount(self, key, missing_type):
		if type(missing_type) == list:
			count = 0
			for miss_type in missing_type:
				count += db[collname].find({key:missing_type}).count()
			return count
		else:
			return db[collname].find({key:missing_type}).count()

	# Complete This Function
	# def getOutliers(self, key, threshold):
		# distincts = self.getDistinct(key)


	def bivariate_analysis(self, key1, key2, group_key, collname, limit = False, sorting_order = "DESC"):
		''' 
		Variable Analysis - BiVariate 

		Function to perform bivariate analysis on a variable. 

		params:
		key1: Name of the key1 (variable)
		key2: Name of the key2 (variable) 
		collname: Name of the collection which contains the documents
		limit: Number of documents / rows to be analysed, Default is False (all documents)
		sorting_order: Arranging the results in asending or descending order (ASEC or DESC)
		'''

		sorter = -1
		if sorting_order != "DESC":
			sorter = 1

		pipe = [{'$group' : {'_id' : {'key1':'$'+key1,'key2':'$'+key2}, 'freq' : {'$sum':'$'+group_key}, 'mean':{'$avg':'$'+group_key}, 'min':{'$min':'$'+group_key}, 'max':{'$max':'$'+group_key} }}, 
				{'$sort':{'freq':sorter}}]
		if limit:
			pipe.append({'$limit':limit})
	
		res = db[collname].aggregate(pipe)
		res = self.cursor_to_list(res)
		return res 

	def createBins(self, listofdicts, key, window_size, scaler):
		bins = {}
		for x in listofdicts:
			amt = x['_id']

			if not amt:
				continue

			amt = amt.replace("%","").replace("'","").strip()

			if "." in amt:
				bucketed = math.floor(float(amt))
			else:
				bucketed = float(amt)

			bucketed = bucketed / scaler
			bucket = str(math.floor(bucketed / window_size) * window_size)

			if bucket not in bins:
				bins[bucket] = {}
				bins[bucket]['bucket_name'] = float(bucket)
				bins[bucket]['bucket_data'] = []
				bins[bucket]['bucket_sums'] = []
			bins[bucket]['bucket_data'].append(x['sum'])
			bins[bucket]['bucket_sums'].append(x['_id'])

		binslist = [bins[each] for each in bins]
		sortedbins = sorted(binslist, key=lambda k: k['bucket_name']) 
		return sortedbins



	def createBinsBiVariate(self, listofdicts, key1, window_size1, scaler1, key2, window_size2, scaler2):
		bins = {}
		for x in listofdicts:
			if "key1" not in x['_id'] or "key2" not in x['_id']:
				continue

			key1 = x['_id']['key1']
			if not key1:
				continue

			bucket_key1 = key1
			if window_size1:
				key1 = key1.replace("%","").replace("'","").strip()
				if "." in key1:
					bucketed_key1 = math.floor(float(key1))
				else:
					bucketed_key1 = float(key1)
				bucketed_key1 = bucketed_key1 / scaler1
				bucket_key1 = str(math.floor(bucketed_key1 / window_size1) * window_size1)


			key2 = x['_id']['key2']
			if not key2:
				continue

			bucket_key2 = key2
			if window_size2:
				key2 = key2.replace("%","").replace("'","").strip()
				if "." in key2:
					bucketed_key2 = math.floor(float(key2))
				else:
					bucketed_key2 = float(key2)
				bucketed_key2 = bucketed_key2 / scaler2
				bucket_key2 = str(math.floor(bucketed_key2 / window_size2) * window_size2)

			if bucket_key1 not in bins:
				bins[bucket_key1] = {}

			if bucket_key2 not in bins[bucket_key1]:
				bins[bucket_key1][bucket_key2] = {}
				try:
					bins[bucket_key1][bucket_key2]['bucket_name'] = float(bucket_key2)
				except Exception as E:
					bins[bucket_key1][bucket_key2]['bucket_name'] = bucket_key2

				bins[bucket_key1][bucket_key2]['bucket_data'] = []
			bins[bucket_key1][bucket_key2]['bucket_data'].append(x['sum'])

		sortedbins = {}
		for ky,v in bins.iteritems():	
			newV = sorted(v.values(), key=lambda k: k['bucket_name']) 
			sortedbins[ky] = newV
		return sortedbins

	def loanPerformance(self):
		pipe = [{'$group': {'_id':'$grade', 'idd' : {'$push':'$loan_amnt'}}}]
		for x in db[collname].aggregate(pipe):
			if "idd" in x:
				z = [float(a.replace("%","").strip()) for a in x['idd']] 
				if z:
					avv = sum(z)
					print x['_id'] + "\t" + str(avv)


		# pipe = [{'$group' : { '_id'	  : {'grade' : '$grade', 'status' : '$loan_status'}, 
		# 					  'count' : {'$sum':1},
		# 					  'principals' : {'$push':'$total_rec_prncp'},
		# 					  'interests' : {'$push':'$total_rec_int'},
		# 					  'int_rate' : {'$push':'$int_rate'},
		# 					} 
		# 		}]
		# res = db[collname].aggregate(pipe)
		# for each in res:
		# 	princ = sum([float(x) for x in each['principals']])
		# 	intrs = sum([float(x) for x in each['interests']])
		# 	intt = [float(x.replace("%","").strip()) for x in each['int_rate']]
		# 	if len(intt):
		# 		int_rate = float(sum(intt)) / len(intt)

		# 		print each['_id']['status'] +"\t"+ each['_id']['grade'] +"\t"+  str(each['count']) +"\t"+  str(princ) +"\t"+  str(intrs) +"\t"+  str(int_rate)




class Visualize:

	def create_univariate_table(self, listofdicts, key):
		print key + "\tFreq\tMin\tMax\tMean"
		for each in listofdicts:
			if "min" in each:
				print str(each['_id']) +"\t"+ str(each['freq']) + "\t" + str(each['min'])+ "\t" + str(each['max'])+ "\t" + str(each['mean'])
			else:
				print str(each['_id']) +"\t"+ str(each['freq'])


	def createBiTable(self, listofdicts, key1, key2):
		print key1, key2
		for each in listofdicts:
			if "key1" not in each['_id'] or "key2" not in each['_id']:
				continue
			
			print str(each['_id']['key1']) +"\t"+ str(each['_id']['key2']) +"\t"+ str(each['freq']) + "\t" + str(each['min'])+ "\t" + str(each['max'])+ "\t" + str(each['mean'])


	def printBinsTable(self, bins, key, window_size):
		print "Variable:\t"+str(key)
		print "Bucket\tCounts\tSum\tAvgSum\tMin\tMax\tTotalSums"
		for each in bins:

			xval = str(each['bucket_name']).replace(".0","")

			yval = each['bucket_data']
			if type(yval[0]) != int:
				yval = [float(a.replace("%","")) for a in yval]

			zval = each['bucket_sums']
			if type(zval[0]) != int:
				zval = [float(a.replace("%","")) for a in zval]

			print str(xval)+"-"+str(int(xval)+window_size) +"\t"+ str(sum(yval)) +"\t"+ str(sum(zval)) +"\t"+ str(float(sum(zval))/len(zval)) +"\t"+ str(min(zval)) +"\t"+ str(max(zval)) +"\t"+ str(len(zval))


	def printBinsTableBi(self, bins, k1, k2, w1, w2):		
		rowX = bins.keys()
		rowY = [str(x['bucket_name']) for x in bins.values()[0]]

		matrix = {}
		for x,y in bins.iteritems():
			if x not in matrix:
				matrix[x] = {}
			
			for buck in y:
				yval = str(buck['bucket_name'])
				if yval not in matrix[x]:
					matrix[x][yval] = 0
				matrix[x][yval] = sum(buck['bucket_data'])


		print "Variable " + "\t" + "\t".join(rowY)
		for x in rowX:
			print x+"-"+str(float(x)+w1) + "\t", 
			for i, y in enumerate(rowY):
				if y not in matrix[x]:
					print str(0)+"\t",
				else:
					print str(matrix[x][y]) + "\t",
			print   


add = Add()
get = Get()
eda = EDA()
vis = Visualize()

# print eda.identifyVariableType('grade')
# print eda.Univariate('annual_inc')

# window_size = 50
# scaler = 1000
# key = 'title'
# uni = eda.Univariate(key)
# vis.createUniTable(uni, key)
# bins = eda.createBins(uni, key, window_size, scaler)
# vis.printBinsTable(bins, key, window_size)


# k2 = 'loan_amnt'
# window_size2 = 5
# scaler2 = 1000

# k1 = 'annual_inc'
# window_size1 = 20 #None
# scaler1 = 1000 #None

# bi = eda.BiVariate(k1, k2)
# vis.createBiTable(bi, k1, k2)
# bins = eda.createBinsBiVariate(bi, k1, window_size1, scaler1, k2, window_size2, scaler2)
# vis.printBinsTableBi(bins, k1, k2, window_size1, window_size2)





from pymongo import MongoClient 
import math, collections
db = MongoClient()['LoanAnalysis']
collname = 'Data'

class Add:
	def addJson(self, doc):
		try:
			db[collname].insert(doc)
		except Exception as E:
			print E

class Get:
	def getData(self):
		return db[collname].find()

class EDA:

	''' Need to fix this '''
	def identifyVariableType(self, key):
		distinct_count = self.getDistinctCount(key)
		total_count = self.getTotalCount(key)
		ratio_unique = round((float(distinct_count) / total_count) * 100,2)
		return ratio_unique

	# Function to fix the datatype of a variable in mongo
	def fixDataType(self, key, type):
		return None


	''' Univariate Analysis on variable '''
	# TODO - ADD MEDIAN AND MODE
	def Univariate(self, key, limit = False, sorting_order = "DESC", central_tendencies = True):
		sorter = -1
		if sorting_order != "DESC":
			sorter = 1

		if central_tendencies:
			pipe = [{'$group' : {'_id' : '$'+key, 'freq' : {'$sum':1}, 'mean':{'$avg':1}, 'min':{'$min':1}, 'max':{'$max':1}}}, 
					{'$sort':{'sum':sorter}}]
		else:
			pipe = [{'$group' : {'_id' : '$'+key, 'freq' : {'$sum':1}}},
					{'$sort':{'sum':sorter}}]

		if limit:
			pipe.append({'$limit':limit})

		res = db[collname].aggregate(pipe)
		res = self.cursor_to_list(res)
		return res 

	def getDistinct(self, key):
		return db[collname].distinct(key)

	def getDistinctCount(self, key):
		return len(self.getDistinct(key))

	def getTotalCount(self, key):
		return db[collname].find().count()

	def cursor_to_list(self, cursor):
		return 	[_ for _ in cursor]

	''' Get BiVariate Distributions '''
	def BiVariate(self, key1, key2, limit = False, sorting_order = "DESC"):
		sorter = -1
		if sorting_order != "DESC":
			sorter = 1

		pipe = [{'$group' : {'_id' : {'key1':'$'+key1,'key2':'$'+key2}, 'sum' : {'$sum':1}, 'avg':{'$avg':1}, 'min':{'$min':1}, 'max':{'$max':1} }}, 
				{'$sort':{'sum':sorter}}]
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

	def createUniTable(self, listofdicts, key):
		print key
		for each in listofdicts:
			# if ">" in str(each['_id']):
			# 	print str(each['_id'].split(">")[1]).replace("<br","") * each['sum']

			print str(each['_id']) +"\t"+ str(each['sum']) #+ "\t" + str(each['min'])+ "\t" + str(each['max'])+ "\t" + str(each['avg'])


	def createBiTable(self, listofdicts, key1, key2):
		print key1, key2
		for each in listofdicts:
			if "key1" not in each['_id'] or "key2" not in each['_id']:
				continue
			
			print str(each['_id']['key1']) +"\t"+ str(each['_id']['key2']) +"\t"+ str(each['sum']) + "\t" + str(each['min'])+ "\t" + str(each['max'])+ "\t" + str(each['avg'])


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

print eda.identifyVariableType('grade')
print eda.Univariate('annual_inc')


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





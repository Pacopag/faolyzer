from __future__ import division  #ensures division as double
from pymongo import Connection

def get_land_used_for_production(year,country_code,item_code,quantity,flag=[]):
	"""
	Given a year, country code, and trade item code, and quantity of that item, return the land area
	used to produce that quantity in the given country.
	"""
	if item_code in livestock_codes:
		carcass_weight = get_carcass_weight(year,country_code,item_code)
		quantity *= carcass_weight
		meat_code = livestock_reverse_mappings[item_code]
		return get_land_used_for_production(year,country_code,meat_code,quantity)
		
	source_codes, multipliers, flags = get_source_tree(item_code)
	if 0 in flags or 2 in flags:
		source_yields = {}
		source_ssrs = {}
		source_weights = {}
		for code in source_codes:
			source_yields[code],f=get_yield(year,country_code,code)
			#print "Yield",code,source_yields[code]
			source_ssrs[code],f=get_ssr(year,country_code,code,incl_exports=False)
			#print "SSR",code,source_ssrs[code]
			source_production,f=get_production(year,country_code,code)
			#print "Production",code,source_production
			if isinstance(source_yields[code],dict):
				source_production = source_production['T']
				source_yields[code] = source_yields[code]['T']
			source_imports,source_exports = get_import_export(year,country_code,code)
			source_weights[code] = source_production+float(source_imports)-float(source_exports)
		sum_weights = sum(source_weights.values())
		#print "Sum weights",sum_weights
		if sum_weights==0:
			sum_weights = float("inf")
		source_weights = {code:(weight/sum_weights) for code,weight in source_weights.iteritems()}
		
		source_displacements = {}
		displacement = 0.0
		for code,multiplier in zip(source_codes,multipliers):
			source_displacements[code] = quantity*float(source_ssrs[code])*float(source_weights[code])/float(source_yields[code])/multiplier if source_yields[code]>0 else 0
			displacement += source_displacements[code]
			
		return displacement,source_displacements
	else:
		return 0.0,{}

def get_offtake_rate(year,country_code,item_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given a year, country_code, and primary livestock item code, return the off-take rate for the associated live animals.
	For non-ruminants, the value defaults to 1.
	"""
	
	if item_code not in bovine_meat_codes+ovine_meat_codes:
		return 1.0,"NR"
		
	if org_year is None:
		org_year = year
		
	#get reported number slaughered
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementgroup':producing_animals_group}
	fields = {'value':1,'flag':1}
	rec,f = find_one(table_productionlivestockprimary,spec,fields)
	num_slaughtered = rec['value'] if rec is not None else 0.0
	
	no_data = num_slaughtered==0
	if no_harvest and next_dir>-1 and year<max_year:  #get next
		next_dir = 1
		#flag.append("Ny")
		return get_offtake_rate(year+1,country_code,item_code,flag,org_year,next_dir,aggregate_level)
	elif num_slaughtered and year==max_year and org_year!=min_year:
		next_dir = -1
		#flag = list(set(flag)-set(['Ny']))#flag.translate(None,'Ny')+"Py"
		#flag.append('Py')
		return get_offtake_rate(org_year-1,country_code,item_code,flag,org_year,next_dir,aggregate_level)
	elif num_slaughtered and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append('Py')
		return get_offtake_rate(year-1,country_code,item_code,flag,org_year,next_dir,aggregate_level)
	elif num_slaughtered and country_code!=world_code:
		#flag = list(set(flag)-set(['Py'])-set(['Ny']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])#'A'+str(region_code)
		return get_offtake_rate(org_year,region_code,item_code,flag,org_year,next_dir,aggregate_level)
	elif no_harvest:
		return 0.0,"No data"
	
	#get number of meat animals	
	num_meat_animals = get_num_animals(year,country_code,item_code,from_db=True)[0]['T']
	
	#get number of milk animals
	milk_code = meat_milkeggs_mappings[item_code]
	num_milk_animals = get_num_animals(year,country_code,milk_code,from_db=True)[0]['T']
	
	#get number of culled milk animals
	cull_rate = get_cull_rate(year,country_code,milk_code,from_db=True)[0]
	num_culled = num_milk_animals*cull_rate

def get_num_animals(year,country_code,item_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given a year, country_code, and primary livestock item_code or livestock item_code, return the number of animals in that system.
	For live animal item, this is just the stocks reported in FAOSTAT with all units converted to "head".
	For milk/egg items, this is the number of producing/laying animals.
	For meat items, this is the larger of number slaughtered and (stocks - milk/eggs animals)
	"""
	
	if from_db:
		(num_animals,flag) = ({'T':0.0,'ML':0.0,'P':0.0},'No data')#(rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code}
		fields = {'elementcode':1,'value':1,'flag':1}
		qry,f = find(table_liveanimalproduction,spec,fields)
		for rec in qry:
			if rec['elementcode']==-2100:
				num_anmials['T'] = rec['value']
			elif rec['elementcode']==-2101:
				num_animals['ML'] = rec['value']
			elif rec['elementcode']==-2102:
				num_animals['P'] = rec['value']
			else:
				print "Invalid elementcode in get_num_animals"
				raise ValueError;
		return num_animals,flag
		
	if org_year is None:
		org_year = year
	
	is_primary = item_code in milkeggsmeat_animal_mappings
	
	# Get stocks of corresponding animal
	animal_code = item_code
	primary_code = item_code
	if is_primary:
		animal_code = milkeggsmeat_animal_mappings[item_code]
	else:
		primary_code = livestock_reverse_mappings[item_code]
	num_stocks = 0
	spec = {'year':year,'countrycode':country_code,'itemcode':animal_code}
	fields = {'elementcode':1,'value':1}
	rec,f = find_one(table_productionlivestock,spec,fields)
	if rec is not None:
		mult = 1000.0 if rec['elementcode']==5112 else 1.0 #convert 1000 head to head
		num_stocks = mult*rec['value']
	
	# For meat,milk and egg codes, get number producing/slaughtered.
	num_producing = 0
	if is_primary:
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementgroup':producing_animals_group}
		fields = {'elementcode':1,'value':1}
		rec,f = find_one(table_productionlivestockprimary,spec,fields)
		if rec is not None:
			mult = 1000.0 if rec['elementcode'] in khead_codes else 1.0
			num_producing = mult*rec['value']
			
	no_data = num_stocks+num_producing==0.0
	if no_data and next_dir>-1 and year<max_year-1:  # the -1 is a band-aid since 2010 land data is not available yet
		next_dir = 1
		#flag = list(set(flag)-set(['Fr']))#flag.translate(None,'Fr') + "Ny"
		#flag.append('Ny')
		return get_num_animals(year+1,country_code,item_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and year==max_year-1 and org_year!=min_year: # the -1 is a band-aid since 2010 land data is not available yet
		next_dir = -1
		#flag = list(set(flag)-set(['Fr'])-set(['Ny']))#flag.translate(None,'Fr') + "Ny"
		#flag.append('Py')
		return get_num_animals(org_year-1,country_code,item_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag = list(set(flag)-set(['Fr']))#flag.translate(None,'Fr') + "Ny"
		#flag.append('Py')
		return get_num_animals(year-1,country_code,item_code,flag,org_year,next_dir,aggregate_level)
	elif no_data:
		return {'T':0.0,'ML':0.0,'P':0.0}, "No data"
	
	yr = year-1970 #Bouwman et al. (2005) data starts at 1970, but the quadratic params a,b,c are fitted to the shifted data where 1970 -> 0
	region_code = get_country_region(country_code)
	spec = {'aggregatecode':{'$in':[region_code,world_code]},'itemcode':primary_code}
	fields = {'a':1,'b':1,'c':1}
	qry2,f2 = find(table_systemanimalfractions,spec,fields,sort=[('aggregatecode',-1)])
	rec2 = qry2.next()
	MLfrac = rec2['a']*yr*yr + rec2['b']*yr + rec2['c'] #fraction of animals from mixed+landless systems
		
		
	# Map itemcode to liveanimal code if meat product
	num = 0
	if item_code in livestock_codes:
		num = num_stocks
	elif item_code in milk_codes+egg_codes:
		num = num_producing
	elif item_code in meat_milkeggs_mappings:
		corresp_code = meat_milkeggs_mappings[item_code]
		num_corresp,f = get_num_animals(year,country_code,corresp_code)
		num_corresp = num_corresp['T']
		
		num = num_stocks - num_corresp
		if num_producing > num:
			num = num_producing
	else:
		num = num_producing if num_producing > num_stocks else num_stocks
		
	num_animals = num
	num_animals_ML = MLfrac*num
	num_animals_P = num_animals - num_animals_ML
	
	flag = ''
	ret = {'T':num_animals,'ML':num_animals_ML,'P':num_animals_P}
	
	return ret,flag

def get_stocking_rate(year,country_code,item_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given a year, country_code, and primary ruminant livestock item_code, return the number of animals per hectare of pasture
	"""
	
	if from_db:
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code}
		fields = {'value':1,'flag':1}
		rec = find_one(table_stockingrates,spec,fields)
		(stocking_rate,flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		return stocking_rate,flag
		
	pa,f = get_pasture_areas(year,country_code,item_code)
	pasture_area = pa['T']
	pasture_area_ML = pa['ML']
	pasture_area_P = pa['P']
	
	na,f = get_num_animals(year,country_code,item_code)#,from_db=True)
	num_animals = na['T']
	num_animals_ML = na['ML']
	num_animals_P = na['P']
	
	#print pasture_area_P, num_animals_P
	
	stocking_rate = num_animals/pasture_area if pasture_area!=0 else 0.0
	stocking_rate_ML = num_animals_ML/pasture_area_ML if pasture_area_ML!=0 else 0.0
	stocking_rate_P = num_animals_P/pasture_area_P if pasture_area_P!=0 else 0.0
	
	flag = ''
	ret = {'T':stocking_rate,'ML':stocking_rate_ML,'P':stocking_rate_P}
	
	return ret,flag
	

def get_weighted_yield(year,country_code,item_codes,sector='total',sys_code=-5511,imports=True,exports=True,cull=False,flag=[],org_year=None,next_dir=0,aggregate_level=0,get_next=False):
	"""
	Get the average yield of primary commodities specified by item_codes.
	"""
	production = 0.0
	area_harvested = 0.0
	for item_code in item_codes:
		p,p_flag = get_production(year,country_code,item_code,sys_code,imports,exports,cull,from_db=True)
		a,a_flag = get_area_harvested(year,country_code,item_code,sector,get_next=get_next,from_db=True)
		if isinstance(p,dict): #livestock products return dictionaries...get only total "T" component
			p = p['T']
			a = a['T']
		production += p
		area_harvested += a
	
	wyield = production/area_harvested if area_harvested!=0 else 0.0
	flag = ''
	return wyield,flag


def get_livestock_stats(year,country_code,item_code,flag='',org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given a year country_code and milk or egg item code, get the fraction of milk/laying animals that
	were likely culled for meat during the given year.
	"""
	if from_db:
		stats = {
			'stocks':0,
			'meat_animals':0,
			'meat_animals_ML':0,
			'meat_animals_P':0,
			'producing_animals_T':0,
			'producing_animals_ML':0,
			'producing_animals_P':0,
			'births':0,
			'meat_births':0,
			'dairyegg_births':0,
			'old_maids':0,
			'slaughtered':0,
			'offtake_rate':0,
			'offtake_rate_ML':0,
			'offtake_rate_P':0,
			'carcass_weight':0,
			'carcass_weight_ML':0,
			'carcass_weight_P':0,
			'cull':0,
			'cull_rate':0
		}
		flag = "No data"
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code}
		fields = {'value':1,'flag':1}
		rec = find_one(table_livestockstats,spec,fields)
		
		return stats,flag
	
	if org_year is None:
		org_year = year
	
	if year == min_year:
		year += 1
	elif year == max_year:
		year -= 1
		
	is_milkegg_animal = True
	milkegg_code = None
	try:
		if item_code in livestock_codes:
			animal_code = item_code
			meat_code = animal_meat_mappings[item_code]
			milkegg_code = animal_milkeggs_mappings[item_code]
		elif item_code in milk_codes+egg_codes:
			milkegg_code = item_code
			meat_code = milkeggs_meat_mappings[item_code]
			animal_code = milkeggs_animal_mappings[item_code]
		elif item_code in meat_codes:
			meat_code = item_code
			animal_code = meat_animal_mappings[item_code]
			milkegg_code = meat_milkeggs_mappings[item_code]
		else:
			print item_code,"is an invalid item code for get_livestock_stats"
			raise ValueError
	except KeyError:
		is_milkegg_animal = False
		
	mult = 1000.0 if milkegg_code in egg_codes else 1.0
	
	
	# Get animal stock
	spec = {'year':{'$in':[year-1,year,year+1]},'countrycode':country_code,'itemcode':animal_code}
	fields = {'year':1,'value':1}
	qry,f = find(table_productionlivestock,spec,fields)
	(stocks,last_stocks,next_stocks) = (0.0, 0.0, 0.0)
	for rec in qry:
		if rec['year']==year:
			stocks = mult*rec['value']
		elif rec['year']==year+1:
			next_stocks = mult*rec['value']
		elif rec['year']==year-1:
			last_stocks = mult*rec['value']
	
	
	#Get live animal import/export
	cc = country_code if country_code!=china_producing_code else china_trade_code
	spec = {'year':{'$in':[year-1,year,year+1]},'countrycode':cc,'itemcode':animal_code,'elementcode':{'$in':import_codes+export_codes}}
	fields = {'year':1,'elementcode':1,'value':1}
	qry,f = find(table_tradeliveanimals,spec,fields)
	(trade,last_trade,next_trade) = (0.0,0.0,0.0)
	for rec in qry:
		if rec['elementcode'] in import_codes:
			if rec['year']==year:
				trade += mult*rec['value']
			elif rec['year']==year+1:
				next_trade += mult*rec['value']
			elif rec['year']==year-1:
				last_trade += mult*rec['value']
		elif rec['elementcode'] in export_codes:
			if rec['year']==year:
				trade -= mult*rec['value']
			elif rec['year']==year+1:
				next_trade -= mult*rec['value']
			elif rec['year']==year-1:
				last_trade -= mult*rec['value']
			
	
	# Domestic stock after trade
	domestic = stocks+trade
	last_domestic = last_stocks+last_trade
	next_domestic = next_stocks+next_trade
	
	# Get number of animals slaughtered
	spec = {'year':{'$in':[year-1,year,year+1]},'countrycode':country_code,'itemcode':meat_code,'elementgroup':producing_animals_group}
	fields = {'year':1,'value':1}
	qry,f = find(table_productionlivestockprimary,spec,fields)
	(slaughtered,next_slaughtered,last_slaughtered) = (0.0,0.0,0.0)
	for rec in qry:
		if rec['year']==year:
				slaughtered = mult*rec['value']
		elif rec['year']==year+1:
			next_slaughtered = mult*rec['value']
		elif rec['year']==year-1:
			last_slaughtered = mult*rec['value']
			
	# No data condition
	if stocks==0 or next_domestic==0:
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])#'A'+str(region_code)
		flag = 'A'
		return get_livestock_stats(org_year,region_code,item_code,flag,org_year,next_dir,aggregate_level)
			
	# We can stop here if related animal is meat-only
	if not is_milkegg_animal:
		annual_stocks1 = slaughtered+next_stocks
		annual_stocks2 = stocks
		annual_stocks = max([annual_stocks1,annual_stocks2])
		
		births1 = annual_stocks - domestic
		last_survivors = last_domestic - last_slaughtered
		births2 = stocks - last_survivors
		births = max([births1,births2])
		
		offtake_rate = slaughtered/stocks
		
		production,f = get_livestockprimary_production(year,country_code,meat_code,imports=False,exports=False,cull=True)
		production_T = production['T']
		carcass_weight = production_T/slaughtered if slaughtered!=0 else get_carcass_weight(year,country_code,animal_code)
		
		return {
			'stocks':annual_stocks,
			'meat_animals':annual_stocks,
			'meat_animals_ML':annual_stocks,
			'meat_animals_P':0,
			'producing_animals_T':0,
			'producing_animals_ML':0,
			'producing_animals_P':0,
			'births':births,
			'meat_births':births,
			'dairyegg_births':0,
			'old_maids':0,
			'slaughtered':slaughtered,
			'offtake_rate':offtake_rate,
			'offtake_rate_ML':offtake_rate,
			'offtake_rate_P':0,
			'carcass_weight':carcass_weight,
			'carcass_weight_ML':carcass_weight,
			'carcass_weight_P':0,
			'cull':0,
			'cull_rate':0
		},flag
	
	# Get number of producing animals
	spec = {'year':{'$in':[year-1,year,year+1]},'countrycode':country_code,'itemcode':milkegg_code,'elementgroup':producing_animals_group}
	fields = {'year':1,'value':1}
	qry,f = find(table_productionlivestockprimary,spec,fields)
	(producing,next_producing,last_producing) = (0.0,0.0,0.0)
	for rec in qry:
		if rec['year']==year:
				producing = mult*rec['value']
		elif rec['year']==year+1:
			next_producing = mult*rec['value']
		elif rec['year']==year-1:
			last_producing = mult*rec['value']
	
	# Here's the meat
	if milkegg_code in milk_codes:
		survivors = domestic - slaughtered	
		next_births = next_stocks - survivors
		next_dairy_share = next_producing/next_domestic
		next_dairy_births = next_births*next_dairy_share
		next_old_maids = next_producing - next_dairy_births		
		cull = producing - next_old_maids
		cull_rate = cull/producing if producing!=0 else 0.0
		
		last_survivors = last_domestic - last_slaughtered
		births = stocks - last_survivors
		dairyegg_share = producing/domestic
		dairyegg_births = births*dairyegg_share
		meat_births = births - dairyegg_births
		old_maids = producing - dairyegg_births
		annual_stocks = stocks
		off_take_rate = (slaughtered-cull)/(stocks - producing) if (stocks - producing)>0 else 0.0
		
	elif milkegg_code in egg_codes:
		annual_stocks = slaughtered+next_stocks
		dairyegg_share = producing/annual_stocks if annual_stocks>0 else 1.0
		births = annual_stocks - domestic
		dairyegg_births = births*dairyegg_share
		next_old_maids = next_producing - dairyegg_births
		cull = producing-next_old_maids
		cull_rate = cull/producing if producing!=0 else 0.0
		
		last_annual_stocks = last_slaughtered+stocks
		last_dairyegg_share = last_producing/last_annual_stocks
		last_births = last_annual_stocks - last_domestic
		last_dairyegg_births = last_births*last_dairyegg_share
		old_maids = producing - last_dairyegg_births
		meat_births = births - dairyegg_births
		off_take_rate = 1
	else:
		raise ValueError
	
	if cull_rate < 0:
		cull_rate = 0.0
	elif cull_rate > 1:
		cull_rate = 1.0
	
	yr = year-1970 #Bouwman et al. (2005) data starts at 1970, but the quadratic params a,b,c are fitted to the shifted data where 1970 -> 0
	region_code = get_country_region(country_code)
	spec = {'aggregatecode':{'$in':[region_code,world_code]},'itemcode':meat_code}
	fields = {'a':1,'b':1,'c':1}
	qry2,f2 = find(table_systemanimalfractions,spec,fields,sort=[('aggregatecode',-1)])
	rec2 = qry2.next()
	MLfrac_animals = rec2['a']*yr*yr + rec2['b']*yr + rec2['c'] #fraction of animals from mixed+landless systems
	
	qry2,f2 = find(table_systemslaughterfractions,spec,fields,sort=[('aggregatecode',-1)])
	rec2 = qry2.next()
	MLfrac_slaughter = rec2['a']*yr*yr + rec2['b']*yr + rec2['c'] #fraction of animals from mixed+landless systems
	
	production,f = get_livestockprimary_production(year,country_code,meat_code,imports=False,exports=False,cull=True)
	production_T = production['T']
	production_ML = production['ML']
	production_P = production['P']
	
	slaughtered_T = slaughtered - cull
	slaughtered_ML = MLfrac_slaughter*slaughtered_T
	slaughtered_P = (1-MLfrac_slaughter)*slaughtered_T
	
	carcass_weight = production_T/slaughtered_T if slaughtered_T!=0 else get_carcass_weight(year,country_code,animal_code)
	carcass_weight_ML = production_ML/slaughtered_ML if slaughtered_ML!=0 else carcass_weight
	carcass_weight_P = production_P/slaughtered_P if slaughtered_P!=0 else 0.0
	
	meat_animals_T = annual_stocks - producing
	meat_animals_ML = MLfrac_animals*meat_animals_T
	meat_animals_P = (1-MLfrac_animals)*meat_animals_T
	
	offtake_rate = slaughtered_T/meat_animals_T if meat_animals_T!=0 else 0.0
	offtake_rate_ML = slaughtered_ML/meat_animals_ML if meat_animals_ML!=0 else 0.0
	offtake_rate_P = slaughtered_P/meat_animals_P if meat_animals_P!=0 else 0.0
	
	producing_ML = MLfrac_animals*producing
	producing_P = (1-MLfrac_animals)*producing
		
	stats = {
		'stocks':annual_stocks,
		'meat_animals':meat_animals_T,
		'meat_animals_ML':meat_animals_ML,
		'meat_animals_P':meat_animals_P,
		'producing_animals_T':producing,
		'producing_animals_ML':producing_ML,
		'producing_animals_P':producing_P,
		'births':births,
		'meat_births':meat_births,
		'dairyegg_births':dairyegg_births,
		'old_maids':old_maids,
		'slaughtered':slaughtered_T,
		'offtake_rate':offtake_rate,
		'offtake_rate_ML':offtake_rate_ML,
		'offtake_rate_P':offtake_rate_P,
		'carcass_weight':carcass_weight,
		'carcass_weight_ML':carcass_weight_ML,
		'carcass_weight_P':carcass_weight_P,
		'cull':cull,
		'cull_rate':cull_rate
	}
	return stats,flag
		
def get_livestockprimary_yield(year,country_code,lp_code,imports=True,exports=True,cull=False,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given a year country_code and primary livestock item code, return the yield as tonnes per hectare of land
	used to produce the primary livestock item.
	"""
	if from_db:
		(lpy,lpy_flag) = ({"T":0.0,"P":0.0,"ML":0.0,"C":0.0},"No data")
		spec = {'year':year,'countrycode':country_code,'itemcode':lp_code}#,'elementcode':sys_code}
		fields = {'elementcode':1,'value':1,'flag':1}
		qry,f = find(table_livestockyields,spec,fields)
		for rec in qry:
			if rec['elementcode']==-5419:
				lpy['T']=rec['value']
			elif rec['elementcode']==-5416:
				lpy['C']=rec['value']
			elif rec['elementcode']==-5417:
				lpy['P']=rec['value']
			elif rec['elementcode']==-5418:
				lpy['ML']=rec['value']
			else:
				print "Invalid elementcode in livestockareaharvested"
				raise ValueError
		lpy_flag = ''
		return lpy,lpy_flag
		
	if org_year is None:
		org_year = year
	
	"""if sector=="total":
		sys_code = -5511
	elif sector=="crop":
		sys_code = -5512
	elif sector=="pasture":
		sys_code = -5513
	"""
	production,lpp_flag = get_livestockprimary_production(year,country_code,lp_code=lp_code,imports=imports,exports=exports,cull=cull)
	#production_T = production['T']
	#production_ML = production['ML']
	#production_P = production['P']
	#if lpp_flag!='':
	#	flag.extend(["P",lpp_flag,"P"])
	#production = productions["T"][lp_code]
	
	area_harvested,ah_flag = get_livestockprimary_area_harvested(year,country_code,lp_code,from_db=True)
	area_harvested.update((k,float(v)) for k,v in area_harvested.items()) #because scientific notation is stored as unicode in mongo
	#area_harvested_T = area_harvested['T']
	#area_harvested = area_harvested["total"]
	#if ah_flag!='':
	#	flag.extend(["Ah",ah_flag,"Ah"])
	
	no_harvest = sum(area_harvested.values())==0
	if no_harvest and next_dir>-1 and year<max_year:  #get next
		next_dir = 1
		#flag.append("Ny")
		return get_livestockprimary_yield(year+1,country_code,lp_code,imports,exports,cull,flag,org_year,next_dir,aggregate_level)
	elif no_harvest and year==max_year and org_year!=min_year:
		next_dir = -1
		#flag = list(set(flag)-set(['Ny']))#flag.translate(None,'Ny')+"Py"
		#flag.append('Py')
		return get_livestockprimary_yield(org_year-1,country_code,lp_code,imports,exports,cull,flag,org_year,next_dir,aggregate_level)
	elif no_harvest and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append('Py')
		return get_livestockprimary_yield(year-1,country_code,lp_code,imports,exports,cull,flag,org_year,next_dir,aggregate_level)
	elif no_harvest and country_code!=world_code:
		#flag = list(set(flag)-set(['Py'])-set(['Ny']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])#'A'+str(region_code)
		return get_livestockprimary_yield(org_year,region_code,lp_code,imports,exports,cull,flag,org_year,next_dir,aggregate_level)
	elif no_harvest:
		return 0.0,"No data"
		
	#print year,country_code,lp_code
	yld_T = production['T']/area_harvested['T'] if area_harvested['T']!=0 else 0.0
	yld_C = production['ML']/area_harvested['C'] if area_harvested['C']!=0 else 0.0
	yld_ML = production['ML']/(area_harvested['P_ML']+area_harvested['C']) if (area_harvested['P_ML']+area_harvested['C'])!=0 else 0.0
	yld_P = production['P']/area_harvested['P_P'] if area_harvested['P_P']!=0 else 0.0
	"""try:
		yld = production/float(area_harvested) if area_harvested!=0 else float('inf')
	except TypeError:
		print year,country_code,lp_code,area_harvested
		raise
	"""
	#flag = ''.join(flag)
	flag = ''
	yld = {'T':yld_T,'C':yld_C,'ML':yld_ML,'P':yld_P}
	return yld,flag
	
def get_feed_ssr(year,country_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given year, country_code and primary livestock code, return the self-suffiency ration of all feed components.  This
	returns a single value for all components weighted according the component's proportion in the feed.
	"""
	if from_db:
		spec={'year':year,'countrycode':country_code}
		fields={'value':1,'flag':1}
		rec,f = find_one(table_feedssr,spec,fields)
		(feed_ssr,flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,'No data')
		return feed_ssr,flag
		
	if org_year is None:
		org_year = year
	
	try_codes = [867,1058,1035,882]
	
	feed_ssr = 0.0
	for lp_code in try_codes:
		feed_quantities,fq_flag = get_feed_quantities(year,country_code,lp_code)
		total_feed = sum(feed_quantities.values())
		if total_feed==0:
			continue
		feed_props = {k:v/total_feed for k,v in feed_quantities.iteritems()}
		for k,v in feed_quantities.iteritems():
			prop = v/total_feed
			ssr,ssr_flag = get_ssr(year,country_code,k,from_db=True)
			#print k,v,prop,ssr
			ssr = float(ssr)
			feed_ssr += prop*ssr if abs(ssr)<5 else 0.0 #the <5 condition just drops anomalies
		if feed_ssr!=0:
			break
	
	no_data = feed_ssr==0	
	"""if no_data and next_dir>-1 and year<max_year:  #get next
		next_dir = 1
		#flag.append("Ny")
		return get_feed_ssr(year+1,country_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and year==max_year and org_year!=min_year:
		next_dir = -1
		#flag = list(set(flag)-set('Ny'))#flag.translate(None,'Ny')+"Py"
		#flag.append("Py")
		return get_feed_ssr(org_year-1,country_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append("Py")
		return get_feed_ssr(year-1,country_code,flag,org_year,next_dir,aggregate_level)
	elif no_data:
		#flag = list(set(flag)-set(['Py'])-set(['Ny']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])
		return get_feed_ssr(org_year,region_code,flag,org_year,next_dir,aggregate_level)
	"""
	if no_data and country_code!=world_code:
		#flag = list(set(flag)-set(['Py'])-set(['Ny']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])
		return get_feed_ssr(org_year,region_code,flag,org_year,next_dir,aggregate_level)	
	flag = ''
	return feed_ssr,flag
	#feed_items_in_production

def get_ssr(year,country_code,item_code,incl_exports=True,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given a year and country code and item_code, return the self-sufficiency ratio (i.e. production/domestic_supply).
	item_code may be from either commodity balance or production, but the corresponding dataset will be used.  If in doubt
	be sure to use the production item codes (e.g. 56 instead of 2514 for maize)
	
	Note: animal codes are mapped to corresponding meat codes
	
	To do: Handle item_code mapping automatically.  Need dictionary of mappings for all items.
	
	"""
	
	if item_code in [1158,1150]:  #This is a hack.  Should instead delete this condition and delete all records in CommodityTrees.csv where source is 1158.
		return 0.0,'No data'
	
	if item_code in livestock_codes:
		item_code = livestock_reverse_mappings[item_code]
	
	if from_db:
		spec={'year':year,'countrycode':country_code,'itemcode':item_code}
		fields={'value':1,'flag':1}
		rec,f = find_one(table_ssr,spec,fields)
		(ssr,flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,'No data')
		return ssr,flag
	
	if org_year is None:
		org_year = year
		
	ssr = None
	reported = False
		
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':{'$in':[domestic_supply_code,production_code]}}
	fields = {'elementcode':1,'value':1}
	qry = table_commoditybalance.find(spec,fields,sort=[('elementcode',1)])
	for rec in qry:
		reported = True
		if rec['elementcode']==domestic_supply_code:
			ssr = rec['value']
		elif rec['elementcode']==production_code and ssr is not None:
			ssr = 1.0*rec['value']/ssr
			
	if not reported: #not a reporter
		ssr = 0.0
		if item_code in crop_codes:
			table = table_cropproduction
		elif item_code in livestockprimary_codes:
			table = table_livestockproductionimportexport
		
		trade_item_code = item_code
		trade_item_conv = 1.0
		if item_code in trade_to_production_mappings:
			trade_item_code = trade_to_production_mappings[item_code][0]
			trade_item_conv = trade_to_production_mappings[item_code][1]
		if item_code in fodder_to_crop_mappings:
			item_code = fodder_to_crop_mappings[item_code]
			trade_item_code = item_code
			
		
		cc = country_code if country_code!=china_producing_code else china_trade_code
		spec = {'year':year,'countrycode':cc,'itemcode':trade_item_code,'elementcode':{'$in':import_codes+export_codes}}
		fields = {'elementcode':1,'value':1}
		qry = table_tradecropslivestock.find(spec,fields,sort=[('elementcode',-1)])
		for rec in qry:
			reported = True
			if rec['elementcode'] in import_codes:
				ssr += trade_item_conv*float(rec['value'])
			elif rec['elementcode'] in export_codes and incl_exports:
				ssr -= trade_item_conv*float(rec['value'])
				
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':{'$in':[production_code,-5511]}}#the -5511 is total production in livestockproduction
		fields = {'elementcode':1,'value':1}
		qry = table.find(spec,fields,sort=[('elementcode',-1)])
		for rec in qry:
			reported = True
			if rec['value']+ssr!=0:
				ssr = rec['value']/(rec['value']+ssr)
			elif rec['value']+ssr==0 and rec['value']!=0:
				ssr = float('inf')
			else:
				ssr = 0.0
		
	#if no data is reported
	if not reported and next_dir>-1 and year<max_year:  #get next
		next_dir = 1
		#flag.append("Ny")
		return get_ssr(year+1,country_code,item_code,incl_exports,flag,org_year,next_dir,aggregate_level)
	elif not reported and year==max_year and org_year!=min_year:
		next_dir = -1
		#flag = list(set(flag)-set('Ny'))#flag.translate(None,'Ny')+"Py"
		#flag.append("Py")
		return get_ssr(org_year-1,country_code,item_code,incl_exports,flag,org_year,next_dir,aggregate_level)
	elif not reported and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append("Py")
		return get_ssr(year-1,country_code,item_code,incl_exports,flag,org_year,next_dir,aggregate_level)
	elif not reported:
		#flag = list(set(flag)-set(['Py'])-set(['Ny']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])
		return get_ssr(org_year,region_code,item_code,incl_exports,flag,org_year,next_dir,aggregate_level)
	
	#if ''.join(flag)!='':
	#	flag = [str(item_code)]+flag
	flag = ''.join(flag)
	return ssr,flag

def get_feed_quantities(year,country_code,lp_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,domestic=True):
	"""
	Given a year country_code and primary livestock item code, return a dictionary with the key
	being the feed component code (e.g. 56 for maize, etc.) and the value being the quantity of that
	item in the feed used to produce the primary livestock item.  The quantities may be scaled by the
	corresponding self-sufficiency ratios (i.e. if ssr=True)
	
	To do : could to the get next / get previous thing
	To do : create a db collection and modify calls to read from db.
	"""
	
	feed_share,fs_flag = get_feed_shares(year,country_code,lp_code,from_db=True)
	feed_share = float(feed_share)
	#feed_share = feed_shares[lp_code]
	
	#if fs_flag!='':
	#	flag.extend(["FS",fs_flag,"FS"])
	#print year,country_code,lp_code
	feed_quantities = {v:0.0 for v in feed_items_in_production}
	
	fields = {'itemcode':1,'elementcode':1,'value':1}
	spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_items_in_balance}}#,'elementcode':feed_code}
	is_balanced = table_commoditybalance.find(spec,fields).count()
	spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_items_in_production},'elementcode':production_code}
	is_produced = table_productioncrops.find(spec,fields).count()
	if is_balanced:
		spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_items_in_balance},'elementcode':feed_code}
		qry,f = find(table_commoditybalance,spec,fields)
		for rec in qry:
			commodity_item_code = rec['itemcode']
			production_item_code = feed_balance_production_mappings[commodity_item_code]
			feed_quantities[production_item_code] = rec['value']*feed_share
			if domestic:
				ssr,ssr_flag = get_ssr(year,country_code,production_item_code,from_db=False)
				if ssr>1 or ssr<0:
					ssr = 1.0
				feed_quantities[production_item_code] *= ssr
				#if ssr_flag!='':
				#	flag.extend(["SSR",ssr_flag,"SSR"])
			
		
	elif is_produced:
		flag += "P"
		cc = country_code if country_code!=china_producing_code else china_trade_code
		spec = {'year':year,'countrycode':cc,'itemcode':{'$in':feed_items_in_production},'elementcode':{'$in':import_codes+export_codes}}
		qry,f = find(table_tradecropslivestock,spec,fields)
		for rec in qry:
			item_code = rec['itemcode']
			element_code = rec['elementcode']
			if element_code in import_codes:
				feed_quantities[item_code]+=float(rec['value'])
			elif element_code in export_codes:
				feed_quantities[item_code]-=float(rec['value'])
				
		spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_items_in_production},'elementcode':production_code}
		qry,f = find(table_productioncrops,spec,fields)
		for rec in qry:
			item_code = rec['itemcode']
			fdr,fdr_flag = get_feed_to_domestic_ratios(year,country_code,item_code)
			feed_quantities[item_code]+=rec['value']*float(fdr)
			feed_quantities[item_code]*=feed_share
			if domestic:
				ssr,ssr_flag = get_ssr(year,country_code,item_code,from_db=False)
				if ssr>1 or ssr<0:
					ssr = 1.0
				feed_quantities[item_code] *= ssr
				#if ssr_flag!='':
				#	flag.extend(["SSR",ssr_flag,"SSR"])
	else:
		flag = ["No data"]
	
	flag = ''.join(flag)		
	return feed_quantities,flag
		
def get_feed_to_domestic_ratios(year,country_code,crop_code=None,flag=[],org_year=None,next_dir=0,aggregate_level=0):
	"""
	Given a year and countrycode, return a dictionary with the key being a feed component code (e.g. 56 for maize, etc.)
	and the value being the fraction of the domestic supply represented by that feed.
	"""
	if crop_code is not None:
		spec = {'year':year,'countrycode':country_code,'itemcode':crop_code}
		fields = {'value':1,'flag':1}
		rec,f = find_one(table_feedtodomesticratio,spec,fields)
		(fdr,fdr_flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,'No data')
		return fdr,fdr_flag
		
	if org_year is None:
		org_year = year
		
	feed_to_domestic_ratios = {v:0.0 for v in feed_items_in_production}
	
	spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_items_in_balance},'elementcode':{'$in':[feed_code,domestic_supply_code]}}
	fields = {'itemcode':1,'elementcode':1,'value':1}
	qry = table_commoditybalance.find(spec,fields,sort=[('itemcode',1),('elementcode',1)])
	for rec in qry:
		item_code = feed_balance_production_mappings[rec['itemcode']]
		if rec['elementcode']==domestic_supply_code:
			feed_to_domestic_ratios[item_code] = rec['value'] if rec['value']!='' else 0.0 #for some reason, some values are empty strings in commodity balance.
		elif rec['elementcode']==feed_code and feed_to_domestic_ratios[item_code] != 0:
			feed_to_domestic_ratios[item_code] = rec['value']/feed_to_domestic_ratios[item_code]
	#get rid of entries where no feed is reported
	for k,v in feed_to_domestic_ratios.iteritems():
		feed_to_domestic_ratios[k] = v if v<=1.0 else 0.0
		
	#if no production is reported
	no_balance = all(v==0 for v in feed_to_domestic_ratios.values())
	if no_balance and next_dir>-1 and year<max_year:  #get next
		next_dir = 1
		#flag.append("Ny")
		return get_feed_to_domestic_ratios(year+1,country_code,crop_code,flag,org_year,next_dir,aggregate_level)
	elif no_balance and year==max_year and org_year!=min_year:
		next_dir = -1
		#flag = list(set(flag)-set(['Ny']))#flag.translate(None,'Ny')+"Py"
		#flag.append("Py")
		return get_feed_to_domestic_ratios(org_year-1,country_code,crop_code,flag,org_year,next_dir,aggregate_level)
	elif no_balance and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append("Py")
		return get_feed_to_domestic_ratios(year-1,country_code,crop_code,flag,org_year,next_dir,aggregate_level)
	elif no_balance:
		#flag = list(set(flag)-set(['Py'])-set(['Ny']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])
		return get_feed_to_domestic_ratios(org_year,region_code,crop_code,flag,org_year,next_dir,aggregate_level)
	
	#flag = ''.join(flag)	
	flag = ''
	return feed_to_domestic_ratios,flag
	
def get_processed_quantity(year,country_code,item_code,flag=[],org_year=None,next_dir=0,aggregate_level=0):
	"""
	Get the quantity of item_code that was reported by country_code to be used for processing in the given year.
	"""
	commodity_code = primary2commodity_mappings[item_code]
	spec = {'year':year,'countrycode':country_code,'itemcode':commodity_code,'elementcode':{'$in':processed_codes}}
	fields = {'value':1}
	rec,f = find_one(table_commoditybalance,spec,fields)
	
	quantity = rec['value'] if rec is not None else 0.0
	
	return quantity
	
def get_livestockprimary_production(year,country_code,lp_code=None,imports=True,exports=True,cull=True,flag=[],org_year=None,next_dir=0,aggregate_level=0,**kwargs):
	"""
	Given a year and country code return a dictionary with the key being the primary livetock
	commodity code and the value being the production adjusted for import/export of liveanimals
	and for culling dairy/egg producing animals.
	"""
	
	if lp_code is not None:
		if imports and exports and cull:
			table = table_livestockproductionimportexportcull
		elif imports and exports:
			table = table_livestockproductionimportexport
		elif exports:
			table = table_livestockproductionexport
		else:
			table = table_livestockproductionnoadj
			
		(lpp_production,lpp_flag) = ({'T':0.0,'ML':0.0,'P':0.0},"No data")
		spec = {'year':year,'countrycode':country_code,'itemcode':lp_code}#,'elementcode':sys_code}
		fields = {'elementcode':1,'value':1,'flag':1}
		qry,f = find(table,spec,fields)
		for rec in qry:
			if rec['elementcode']==-5511:
				lpp_production['T'] = rec['value']
			elif rec['elementcode']==-5512:
				lpp_production['ML'] = rec['value']
			elif rec['elementcode']==-5513:
				lpp_production['P'] = rec['value']
		lpp_flag = ''
		return lpp_production,lpp_flag
	"""if lp_code is not None:
		if imports and exports and cull:
			table = table_livestockproductionimportexportcull
		elif imports and exports:
			table = table_livestockproductionimportexport
		elif exports:
			table = table_livestockproductionexport
		else:
			table = table_livestockproductionnoadj
		spec = {'year':year,'countrycode':country_code,'itemcode':lp_code,'elementcode':sys_code}
		fields = {'value':1,'flag':1}
		rec,f = find_one(table,spec,fields)
		(lpp_production,lpp_flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		return lpp_production,lpp_flag
	"""
	if org_year is None:
		org_year = year
		
	all_codes = bovine_meat_codes+ovine_meat_codes+milk_codes+pig_meat_codes+poultry_meat_codes+egg_codes
	meat_codes = bovine_meat_codes+ovine_meat_codes+pig_meat_codes+poultry_meat_codes
	animal_codes = [livestock_mappings[code] for code in meat_codes]
	milkegg_codes = milk_codes+egg_codes
	
	productions = {code:0 for code in all_codes}
	productions_ML = {code:0 for code in all_codes}
	productions_P = {code:0 for code in all_codes}
	
	#get production of primary livestock products
	spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':all_codes},'elementcode':production_code}
	fields = {'itemcode':1,'value':1}
	qry,f = find(table_productionlivestockprimary,spec,fields)
	for rec in qry:
		productions[rec['itemcode']] = rec['value']  #productions now holds productions
	
	#print "Raw productions",productions	
	#adjust meat productions for import/export of live animals
	if imports or exports:
		cc = country_code if country_code!=china_producing_code else china_trade_code
		spec = {'year':year,'countrycode':cc,'itemcode':{'$in':animal_codes},'elementcode':{'$in':import_codes+export_codes}}
		fields = {'itemcode':1,'elementcode':1,'value':1}
		qry,f = find(table_tradeliveanimals,spec,fields)
		for rec in qry:
			animal_code = rec['itemcode']
			carcass_weight = get_carcass_weight(year,country_code,animal_code)
			item_code = livestock_reverse_mappings[animal_code]
			value = carcass_weight*rec['value']
			if rec['elementcode'] in import_codes and imports:
				productions[item_code]-=value
			elif rec['elementcode'] in export_codes and exports:
				productions[item_code]+=value  #productions now holds productions adjusted for import/export
		
	#print "Import/Export adjustments",productions
	if cull:	
		#adjust meat productions for culling of dairy/egg animals.
		spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':milkegg_codes},'elementgroup':producing_animals_group}
		fields = {'itemcode':1,'elementcode':1,'value':1}
		qry,f = find(table_productionlivestockprimary,spec,fields)
		for rec in qry:
			conv = 1000.0 if rec['elementcode'] in khead_codes else 1.0
			value = conv*rec['value']
			meat_code = milkeggs_meat_mappings[rec['itemcode']]
			animal_code = milkeggs_animal_mappings[rec['itemcode']]
			carcass_weight = get_carcass_weight(year,country_code,animal_code)
			cull_rate = get_livestock_stats(year,country_code,rec['itemcode'])[0]['cull_rate']
			excess = value*carcass_weight*cull_rate
			productions[meat_code]-=excess  #productions now holds productions adjusted for culling.
		#print "Culling adjustments",productions
	
	region_code = get_country_region(country_code)
	
	#remove negative values
	for k,v in productions.iteritems():
		productions[k] = v if v>0 else 0.0
		
	#if no production is reported
	no_production = all(v==0 for v in productions.values())
	#########Following maybe add back in optionally ############
	if no_production and next_dir>-1 and year<max_year:  #get next
		next_dir = 1
		#flag.append("Ny")
		return get_livestockprimary_production(year+1,country_code,lp_code,imports,exports,cull,flag,org_year,next_dir,aggregate_level)
	elif no_production and year==max_year and org_year!=min_year:
		next_dir = -1
		#flag = list(set(flag)-set(['Ny']))
		#flag.append('Py')#flag.translate(None,'Ny')+"Py"
		return get_livestockprimary_production(org_year-1,country_code,lp_code,imports,exports,cull,flag,org_year,next_dir,aggregate_level)
	elif no_production and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append("Py")
		return get_livestockprimary_production(year-1,country_code,lp_code,imports,exports,cull,flag,org_year,next_dir,aggregate_level)
	elif no_production:
		productions = {"T":productions,"ML":productions_ML,"P":productions_P}
		flag = 'No data'
		return productions,flag
		
		
	#split productions into ML and P agri-system
	yr = year-1970 #Bouwman et al. (2005) data starts at 1970, but the quadratic params a,b,c are fitted to the shifted data where 1970 -> 0
	for item_code in all_codes:
		spec = {'aggregatecode':{'$in':[region_code,world_code]},'itemcode':item_code}
		fields = {'a':1,'b':1,'c':1}
		qry,f = find(table_systemproductionfractions,spec,fields,sort=[('aggregatecode',-1)])
		rec = qry.next()
		MLfrac = rec['a']*yr*yr + rec['b']*yr + rec['c'] #fraction of production derived from mixed+landless systems
		productions_ML[item_code] = MLfrac*productions[item_code]
		productions_P[item_code] = (1-MLfrac)*productions[item_code]
		
	if no_production:
		flag = ["No data"]
	
	productions = {"T":productions,"ML":productions_ML,"P":productions_P}
	#flag = ''.join(flag)
	flag = ''
	
	return productions,flag

def get_feed_shares(year,country_code,lp_code=None,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given a year and country code return a dictionary with the key being the primary livetock
	commodity code and the value being the fraction of the country's feed assigned to that commodity.
	"""
	
	if from_db and lp_code is not None:
		spec = {'year':year,'countrycode':country_code,'itemcode':lp_code}
		fields = {'value':1,'flag':1}
		rec,f = find_one(table_feedshares,spec,fields)
		(feed_share,fs_flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		return feed_share,fs_flag
		
	
	if org_year is None:
		org_year = year
	
	all_codes = bovine_meat_codes+ovine_meat_codes+milk_codes+pig_meat_codes+poultry_meat_codes+egg_codes
	meat_codes = bovine_meat_codes+ovine_meat_codes+pig_meat_codes+poultry_meat_codes
	animal_codes = [livestock_mappings[code] for code in meat_codes]
	milkegg_codes = milk_codes+egg_codes
	
	shares,s_flag = get_livestockprimary_production(year,country_code)
	shares = shares["ML"] #just grab the ML
	#if s_flag!='':
	#	flag.extend(["P",s_flag,"P"])
		
	shares[1124] = 0.0 #horses mules and asses feed is negligible (reference!!!)
	shares[1097] = 0.0
	shares[1108] = 0.0
	
	region_code = get_country_region(country_code)
	
	#get feed quantities
	for item_code in all_codes:
		feed_conversion = get_feed_conversion_relative(year,region_code,item_code)
		if item_code in bovine_meat_codes:
			itemtypecode = 0
		elif item_code in milk_codes:
			itemtypecode = 1
		elif item_code in ovine_meat_codes:
			itemtypecode = 2
		elif item_code in pig_meat_codes:
			itemtypecode = 3
		elif item_code in poultry_meat_codes:
			itemtypecode = 4
		elif item_code in egg_codes:
			itemtypecode = 5
		else:
			print "get_feed_shares",year,country_code,item_code
			raise ValueError
		spec = {'aggregatecode':region_code,'itemtypecode':itemtypecode}
		fields = {'value':1}
		rec,f = find_one(table_feedfoodfractions,spec,fields)
		food_frac = rec['value']
		shares[item_code] = shares[item_code]*feed_conversion*food_frac #shares now holds quantity of feed
	
	#normalize the shares wrt the sum
	s = 1.0*sum(shares.values())
	if s==0:
		flag = []#list(set(flag)-set(['Py','Ny'])-set(['P','No data','P']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])
		return get_feed_shares(org_year,region_code,lp_code,flag,org_year,next_dir,aggregate_level)
	shares = {k:v/s for k,v in shares.iteritems()}
	
	#flag = ''.join(flag)
	flag = ''
	return shares,flag
	

def get_feed_crop_area(year,country_code,item_code,production_quantity,include_pasture=True):
	"""
	Given a year, country code, primary livestock code, and primary livestock item quantity,
	this function returns the area of crop land required to produce the feed required
	to produce the given quantity of primary livestock item.
	"""
	if include_pasture:
		print "Including pasture"
	ret_flag = ''
	year = year if year!=2010 else 2009 #band-aid since commodity balances not available for 2010 yet
	
	region_code = get_country_region(country_code)
	#get feed conversion (dry matter to livestock product) for given year, country and item
	feed_conversion = get_feed_conversion(year,region_code,item_code)
	#print "Feed conversion",feed_conversion
	#convert production_quantity into feed quantity
	feed_quantity = production_quantity*feed_conversion#/0.7 #0.7 is conversion of fresh grain to dry matter https://www.google.ca/url?sa=t&rct=j&q=&esrc=s&source=web&cd=3&sqi=2&ved=0CD8QFjAC&url=http%3A%2F%2Fwww.ag.ndsu.edu%2Fextension-aben%2Fdocuments%2Fae905.pdf&ei=lCJCUZycNYTW2AW73YCICg&usg=AFQjCNHbJ2yoaagwMZfa419b3OBOVJcokQ&sig2=13t9lxkISc_MJ3D-jsM1WA
	#print "Feed quantity",feed_quantity
	#get feed compositions for given country and item
	spec = {'aggregatecode':region_code,'itemcode':item_code}
	fields = {'feedcode':1,'value':1}
	qry,flag = find(table_feedmixes,spec,fields)
	#qry = db.feedmixes.find({'aggregatecode':aggregate_code,'itemcode':item_code},{'feedcode':1,'value':1})
	crop_area = 0
	crop_areas = {0:0, 1:0, 2:0, 3:0, 4:0}
	for rec in qry:
		if rec['feedcode']==1 and not include_pasture: #hack until can deal with pasture feed
			continue
			
		feed_components_balance = feedcode_mappings_balance[rec['feedcode']]
		feed_components_production = feedcode_mappings_production[rec['feedcode']]
		
		spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_components_production},'elementcode':production_code}
		total_of_components_production,flag = find_sum(table_productioncrops,spec,'value')
		if flag!='':
			ret_flag += "TP"+flag
		#print "Total of components (production)",total_of_components_production
		
		spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_components_balance},'elementcode':feed_code}
		total_of_components_balance,flag = find_sum(table_commoditybalance,spec,'value')
		#print "SPec",spec
		if flag!='':
			ret_flag +="TB"+flag
		#print "Total of components (balance)",total_of_components_balance
		
		#print "Feedcode",rec['feedcode']
		#print "-------------------------------"
				
		for component_balance,component_production in zip(feed_components_balance,feed_components_production):
			#get production,import and feed quantities reported in commodity balances for each component.
			#print "Component",component_balance,component_production
			#if country_code not in balance_reporters or rec['feedcode']==1:
			
			if total_of_components_balance==0 or rec['feedcode']==1:
				#p = db.productioncrops.find_one({'year':year,'countrycode':country_code,'itemcode':component_production,'elementcode':production_code},{'value':1})['value']
				#i = db.tradecropslivestock.find_one({'year':year,'countrycode':country_code,'itemcode':component_production,'elementcode':{'$in':import_codes}},{'value':1})['value']
				#frac1 = (p/(p+i))  #fraction of supply that is domestic
				
				cc = country_code if country_code!=china_producing_code else china_trade_code
				spec = {'year':year,'countrycode':cc,'itemcode':component_production,'elementcode':{'$in':import_codes}}
				fields = {'value':1}
				rec2,flag2 = find_one(table_tradecropslivestock,spec,fields)
				if flag2!='':
					ret_flag += "I"+flag2
				i = rec2['value'] if rec2 is not None else 0.0
				#print "Imports",i
				
				spec = {'year':year,'countrycode':country_code,'itemcode':component_production,'elementcode':production_code}
				fields = {'value':1}
				rec2,flag2 = find_one(table_productioncrops,spec,fields)
				if flag2!='':
					ret_flag += "P"+flag2
				p = rec2['value'] if rec2 is not None else 0.0
				#print "Production",p
				
				if rec['feedcode']==1:
					frac1 = (p/(p+i)) if p!=0 else 1.0 #fraction that is domestic
					frac2 = (p/total_of_components_production) if total_of_components_production!=0 else 1.0/len(feed_components_production)  #fraction of "feed" attributed to this component
				else:
					frac1 = (p/(p+i)) if p!=0 else 0.0 #fraction that is domestic
					frac2 = (p/total_of_components_production) if total_of_components_production!=0 else 0.0  #fraction of "feed" attributed to this component
					
				feed = feed_quantity*rec['value']*frac1
				#print "Feed",feed
				"""try:
					spec = {'year':year,'countrycode':country_code,'itemcode':component_production,'elementcode':yield_code}
					fields = {'value':1}
					yld = Hg2tonnes*find_one(table_productioncrops,spec,fields,get_next=('year','$gt'))['value']
				except TypeError:
					continue"""
			#elif country_code in balance_reporters:
			elif total_of_components_balance!=0:
				
				spec = {'year':year,'countrycode':country_code,'itemcode':component_balance,'elementcode':{'$in':import_codes+[production_code,feed_code]}}
				fields = {'elementcode':1,'value':1}
				qry2,flag2 = find(table_commoditybalance,spec,fields)
				if flag2!='':
					ret_flag += "B"+flag2
				p,f,i = 0,0,0
				for rec2 in qry2:
					if rec2['elementcode']==production_code:
						p = rec2['value']
						#print "Production",p
					elif rec2['elementcode']==feed_code:
						f = rec2['value']
						#print "Feed balance",f
					else:
						i = rec2['value']
						#print "Import",i
				#print year, country_code, component_balance, total_of_components_balance
				frac1 = (p/(p+i)) if p+i!=0 else 0.0 #fraction of supply that is domestic
				frac2 = (f/total_of_components_balance)# if total_of_components!=0 else 0.0#fraction of feed attributed to this component
				feed = feed_quantity*rec['value']*frac1*frac2
				#print "Feed",feed
			else:
				raise ValueError
			
			yld,yflag = get_yield(year,country_code,component_production)
			if yflag!='':
				ret_flag += "Y"+yflag
			if yld is None or yld==0.0:
				yld,yflag = get_yield(year,world_code,component_production)
				ret_flag += "Y"+yflag+'w'
			#if yld is None or yld==0.0:	
			#	crop_areas[rec['feedcode']] += 0.0
			#	continue
			crop_areas[rec['feedcode']] += feed/yld
			crop_area += feed/yld
	#print crop_areas
	return crop_area,ret_flag
	
def get_feed_conversion_relative(year,region_code,item_code):
	"""
	Given a year, aggregate (region) code, and primary livestock item code
	this function returns the feed conversion rate (i.e. the number of kilograms
	of feed required to produce one kilogram of the primary livestock item.
	"""
	#get conversion parameters (a,b,c are quadratic params fitting Bouwman et al. 2005)
	spec = {'aggregatecode':{'$in':[region_code,world_code]},'itemcode':item_code}
	fields = {'system':1,'a':1,'b':1,'c':1}
	qry,flag = find(table_feedconversionparams,spec,fields,sort=[('aggregatecode',-1)])
	
	yr = year-1970 #Conversion params start at 1970, but the quadratic params a,b,c are fitted to the shifted data where 1970 -> 0
	
	Pconv = 0.0
	for rec in qry:	#There are individual values for Pastoral and Mixed+Landless systems
		if rec['system']=="P":
			Pconv = rec['a']*yr*yr + rec['b']*yr + rec['c'] #feed conversion for pastoral production
		elif rec['system']=="ML":
			MLconv = rec['a']*yr*yr + rec['b']*yr + rec['c'] #feed conversion for mixed+landless production
	
	# Get production fractions for each system
	#spec = {'aggregatecode':{'$in':[region_code,world_code]},'itemcode':item_code}
	fields = {'a':1,'b':1,'c':1}
	#print "spec",spec

	qry,flag = find(table_systemproductionfractions,spec,fields,sort=[('aggregatecode',-1)])
	rec = qry.next()
	#if rec is None:
	#	rec = db.systemfractions.find_one({'aggregatecode':world_code,'itemcode':item_code},{'a':1,'b':1,'c':1})
	
	MLfrac = rec['a']*yr*yr + rec['b']*yr + rec['c'] #fraction of production derived from mixed+landless systems
	
	feed_conversion = Pconv*(1-MLfrac) + MLconv*MLfrac
	
	return feed_conversion

def get_pasture_areas(year,country_code,animal_code=None,flag=[],org_year=None,next_dir=0,aggregate_level=0):
	"""
	Returns a dictionary of key:value pairs where key is the item code of a live animal
	and value is the area of pasture assigned to that animal in the given year and country.
	
	The percentage of the total pasture assigned to each animal is equal to the percentage
	of the total animal population (in livestock units) made up of that animal.
	"""
	if animal_code is not None:
		(pasture_area,flag) = ({'T':0.0,'ML':0.0,'P':0.0},'No data')
		spec = {'year':year,'countrycode':country_code,'itemcode':animal_code}#,'elementcode':sys_code}
		fields = {'elementcode':1,'value':1,'flag':1}
		qry,f = find(table_pastureareas,spec,fields)
		for rec in qry:
			if rec['elementcode']==-3010:
				pasture_area['T']=rec['value']
			elif rec['elementcode']==-3011:
				pasture_area['ML']=rec['value']
			elif rec['elementcode']==-3012:
				pasture_area['P']=rec['value']
			else:
				print "Invalid elementcode in pastureareas"
				raise ValueError
		flag = ''
		return pasture_area,flag
		
	if org_year is None:
		org_year = year
	
	ruminant_codes = bovine_codes+ovine_codes
	ruminant_meat_codes = bovine_meat_codes+ovine_meat_codes
	pasture_areas = {k:0.0 for k in ruminant_meat_codes}	
	pasture_areas_ML = {k:0.0 for k in ruminant_meat_codes}
	pasture_areas_P = {k:0.0 for k in ruminant_meat_codes}	
	#if country_code in country_mappings:
	#	country_code = country_mappings[country_code]
	#	flag += "Cm"
	livestock_units = get_livestock_units(country_code)
	total_pasture_area = 0.0
	#year = year if year!=2010 else 2009 #This is a band-aid since 2010 land data is not available yet
	
	#get total pasture area
	spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':pasture_codes}}
	total_pasture_area,f = find_sum(table_land, spec, 'value')
	total_pasture_area = 1000.0*total_pasture_area #1000.0 prefactor since land area is in kHa
	if total_pasture_area == 0.0: #no data on pasture area, so assume
		#flag.append("Fr")
		spec = {'year':year,'countrycode':country_code,'itemcode':agricultural_land_code,'elementcode':area_code}
		fields = {'value':1}
		rec,f = find_one(table_land,spec,fields)
		#rec = db.land.find_one({'year':yr,'countrycode':country_code,'itemcode':agricultural_land_code,'elementcode':area_code},{'value':1})
		total_pasture_area = 1000.0*0.69*rec['value'] if rec is not None else 0.0 # 0.69 of agricultural land is pasture (world average)
		
	no_data = total_pasture_area==0.0
	if no_data and next_dir>-1 and year<max_year-1:  # the -1 is a band-aid since 2010 land data is not available yet
		next_dir = 1
		#flag = list(set(flag)-set(['Fr']))#flag.translate(None,'Fr') + "Ny"
		#flag.append('Ny')
		return get_pasture_areas(year+1,country_code,animal_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and year==max_year-1 and org_year!=min_year: # the -1 is a band-aid since 2010 land data is not available yet
		next_dir = -1
		#flag = list(set(flag)-set(['Fr'])-set(['Ny']))#flag.translate(None,'Fr') + "Ny"
		#flag.append('Py')
		return get_pasture_areas(org_year-1,country_code,animal_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag = list(set(flag)-set(['Fr']))#flag.translate(None,'Fr') + "Ny"
		#flag.append('Py')
		return get_pasture_areas(year-1,country_code,animal_code,flag,org_year,next_dir,aggregate_level)
	elif no_data:
		flag = "No data"
		pasture_areas = {"T":pasture_areas,"ML":pasture_areas_ML,"P":pasture_areas_P}
		return pasture_areas,flag
	
	#get parts of pasture area that are in ML and P agri-systems
	#P
	region_code = get_country_region(country_code)
	yr = year-1970 #Bouwman et al. (2005) data starts at 1970, but the quadratic params a,b,c are fitted to the shifted data where 1970 -> 0
	spec = {'aggregatecode':{'$in':[region_code,world_code]},'system':'P'}
	fields = {'a':1,'b':1,'c':1}
	qry,f = find(table_systemareafractions,spec,fields,sort=[('aggregatecode',-1)])
	rec = qry.next()
	Pfrac = rec['a']*yr*yr + rec['b']*yr + rec['c'] #fraction grassland in pastoral system
	total_pasture_area_P = total_pasture_area*Pfrac
	#ML    Note that Pfrac != 1-MLfrac because some grassland may be marginal
	spec = {'aggregatecode':{'$in':[region_code,world_code]},'system':'ML'}
	qry,f = find(table_systemareafractions,spec,fields,sort=[('aggregatecode',-1)])
	rec = qry.next()
	MLfrac = rec['a']*yr*yr + rec['b']*yr + rec['c'] #fraction of production derived from mixed+landless systems
	total_pasture_area_ML = total_pasture_area*MLfrac
	total_pasture_area = total_pasture_area_ML + total_pasture_area_P
	
	#print yr,total_pasture_area,total_pasture_area_P,total_pasture_area_ML,Pfrac,MLfrac
	
	total_animals = 0.0
	total_animals_P = 0.0
	total_animals_ML = 0.0
	
	spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':ruminant_codes}}
	fields = {'itemcode':1,'elementcode':1,'value':1}
	qry,f = find(table_productionlivestock,spec,fields)
	for rec in qry:
		mult = 1000.0 if rec['elementcode'] in khead_codes else 1.0 #birds and rodents expressed in 1000 heads
		num_producing_animals = 0
		#item_code = None
		if rec['itemcode'] in milkeggs_animal_mappings.values(): # separate out milk/egg producing animals
			item_code = animal_milkeggs_mappings[rec['itemcode']]
			spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementgroup':producing_animals_group}
			fields = {'value':1}
			r,f = find_one(table_productionlivestockprimary,spec,fields)
			num_producing_animals = r['value'] if r is not None else 0
			#now break these animals up into ML and P parts and convert to livestock units
			spec = {'aggregatecode':{'$in':[region_code,world_code]},'itemcode':item_code}
			fields = {'a':1,'b':1,'c':1}
			qry2,f2 = find(table_systemanimalfractions,spec,fields,sort=[('aggregatecode',-1)])
			rec2 = qry2.next()
			MLfrac = rec2['a']*yr*yr + rec2['b']*yr + rec2['c'] #fraction of animals from mixed+landless systems
			num_livestock_units = mult*livestock_units[rec['itemcode']]*num_producing_animals
				
			#num_producing_animals_ML = mult*livestock_units[rec['itemcode']]*num_producing_animals*MLfrac
			num_producing_animals_ML = num_livestock_units*MLfrac
			pasture_areas_ML[item_code] = num_producing_animals_ML
			#num_producing_animals_P = mult*livestock_units[rec['itemcode']]*num_producing_animals*(1-MLfrac)
			#num_producing_animals_P = num_livestock_units - num_producing_animals_ML
			num_producing_animals_P = num_livestock_units*(1-MLfrac)
			total_animals_ML += num_producing_animals_ML
			pasture_areas_P[item_code] = num_producing_animals_P
			total_animals_P += num_producing_animals_P
			
			pasture_areas[item_code] = num_producing_animals_ML + num_producing_animals_P
			total_animals += num_producing_animals_ML+num_producing_animals_P
		
		#the rest are meat animals	
		num_meat_animals = rec['value']-num_producing_animals
		#now break these animals up into ML and P parts and convert to livestock units
		meat_code = livestock_reverse_mappings[rec['itemcode']]
		spec = {'aggregatecode':{'$in':[region_code,world_code]},'itemcode':meat_code}
		fields = {'a':1,'b':1,'c':1}
		qry2,f2 = find(table_systemanimalfractions,spec,fields,sort=[('aggregatecode',-1)])
		rec2 = qry2.next()
		MLfrac = rec2['a']*yr*yr + rec2['b']*yr + rec2['c'] #fraction of animals from mixed+landless systems
		num_livestock_units = mult*livestock_units[rec['itemcode']]*num_meat_animals
		#num_meat_animals_ML = mult*livestock_units[rec['itemcode']]*num_meat_animals*MLfrac
		num_meat_animals_ML = num_livestock_units*MLfrac
		pasture_areas_ML[meat_code] = num_meat_animals_ML
		total_animals_ML += num_meat_animals_ML
		#num_meat_animals_P = mult*livestock_units[rec['itemcode']]*num_meat_animals*(1-MLfrac)
		num_meat_animals_P = num_livestock_units - num_meat_animals_ML
		pasture_areas_P[meat_code] = num_meat_animals_P
		total_animals_P += num_meat_animals_P
		
		pasture_areas[meat_code] = num_meat_animals_ML + num_meat_animals_P
		total_animals += num_meat_animals_ML+num_meat_animals_P
	
	# Normalize
	if total_animals_P > 0:
		pasture_areas_P = {i:total_pasture_area_P*(p/total_animals_P) for i,p in pasture_areas_P.iteritems()}
	else:
		pasture_areas_P = {i:0 for i,p in pasture_areas_P.iteritems()}
		
	if total_animals_ML > 0:
		pasture_areas_ML = {i:total_pasture_area_ML*(p/total_animals_ML) for i,p in pasture_areas_ML.iteritems()}
	else:
		pasture_areas_ML = {i:0 for i,p in pasture_areas_ML.iteritems()}
		
	if total_animals > 0:
		for i in pasture_areas:
			pasture_areas[i] = pasture_areas_ML[i] + pasture_areas_P[i]
	else:
		pasture_areas = {i:0 for i,p in pasture_areas.iteritems()}
	
	pasture_areas = {"T":pasture_areas,"ML":pasture_areas_ML,"P":pasture_areas_P}
	#flag = ''.join(flag)
	flag = ""
	return pasture_areas,flag
	
def get_production_info(year,country_code,source_codes):
	"""
	Given a year, country code and a list of primary item codes (derived from get_source_tree), 
	this function returns a list of associated yields, and a list of production quantities for 
	the corresponding items.
	
	For primary livestock items, yield is obtained from production divided by the sum associated
	pasture area (see get_pasture_areas) and crop area used to produce feed (see get_feed_crop_area).
	
		-'year' is the year for which to get the info.
		-'country_code' specifies the country for which to get the info.
		-'source_codes' a list of primary item codes.
	"""
	#map partner to associated producer if applicable.
	#country_code = country_mappings[country_code] if country_code in country_mappings else country_code
	if country_code in country_mappings:
		country_code = country_mappings[country_code]
	yields = []
	productions = []
	#pasture_areas = get_pasture_areas(year,country_code)
	livestock_units = None
	for item_code in source_codes:
		#get the yield
		#print year,country_code,item_code
		y,yflag = fetch_yield(year,country_code,item_code)
		#print year,country_code,item_code,y,yflag
		#####y = y if y!=0.0 else None
		yields.append(y)
		#get the production
		p,pflag = get_production(year,country_code,item_code)
		productions.append(p)
	#print "Yields,productions",yields,productions,pasture_areas,animal_code,pasture_area,p
	return yields,productions

			
def get_livestock_units(country_code):
	"""
	A livestock unit is a measure that allows one to compare numbers of different
	livestock species (e.g. 1 goat is equivalent to 0.1 North American cattle).
	The conversion factors vary by world region.  Give a country code, this function
	returns a dictionary of key:value pairs where key is a live animal item code and
	value is the conversion factor of that animal to equivalent livestock units.
	
	This is mainly used to determine stocking rates in terms of required grazing land.
	"""
	value_label = 'value2' #'value2' => poultry, pigs and rodents are assumed landless. Use 'value1' otherwise.
	if country_code in country_mappings:
		country_code = country_mappings[country_code]
	livestock_units = {}
	region_code = get_country_region(country_code)
	spec = {'aggregatecode':region_code}
	fields = {'itemcode':1,value_label:1}
	qry,flag = find(table_livestockunits,spec,fields)
	#qry = db.livestockunits.find({'aggregatecode':aggregate_code},{'itemcode':1,value_label:1})
	for rec in qry:
		livestock_units[rec['itemcode']] = rec[value_label]
	
	return livestock_units	
	
def get_trade_matrix(country_code,item_code,field):
	"""
	Returns a cursor to trade matrix records corresponding to the given reporter and item, sorted by year.
	"""
	try:
		spec = {field:country_code,'itemcode':item_code}
		trade_matrix,flag = find(table_tradematrix,spec,sort=[('year',1)])
		#trade_matrix = db.tradematrix.find({'reportercode':reporter_code,'itemcode':item_code}).sort('year',1)#,{'partnercode':1,'elementcode':1,'value':1,'unit':1})
		return trade_matrix
	except TypeError:
		return None
        	
def get_source_tree(item_code):
	"""
	Many traded commodities are derived from primary (un-processed) commodities.
	Given a target item code, this function returns (sources) a list of primary item
	codes that compose the given target item, (multipliers) a list of corresponding
	conversion factors to convert mass of processed commodity to equivalent mass
	of primary commodity, and (flags) a list of flags indicating if the correponding
	primary commodities are by-products (e.g. Dregs, Bran, etc.), 
	or parts of a compound commodity (i.e. when sources has length greater than 1, 
	e.g. "Breakfast Cereals (41)" is composed of several primary cereal crops).
		-flags:
			0 -> product derived from a single primary commodity (e.g. Wheat flour, Chicken meat)
			1 -> by-product derived from a single primary commodity (e.g. Wheat bran, Chicken fat)
			2 -> product derived from multiple primary commodities (e.g. Breakfast cereals)
			3 -> by-product derived from multiple primary commodities (e.g. Tallow, Dregs)
	"""
	sources = []
	multipliers = []
	flags = [] #"flags" here actually corresponds to "byproduct" in the database
	spec = {'itemcode':item_code}
	qry,flag = find(table_commoditytrees,spec)
	#qry = db.commoditytrees.find({'itemcode':item_code})
	for rec in qry:
		sources.append(rec['parentcode'])
		flags.append(rec['byproduct'])
		m = rec['value'] if rec['byproduct'] not in byproduct_codes else float('inf')
		multipliers.append(m)
	
	if sources==[]:
		return [item_code],[1.0],[0]
	else:
		return sources,multipliers,flags

def get_trade_quantities(year,country_code,source_codes):
	"""
	Given a year, country code and a list of item codes, this function returns
	a list (imports) of import quantities and a list (exports) of the corresponding
	items.
	
	Note: Imports and exports given by this function may be in different from
	those obtained by summing over the trade matrix.
	"""
	imports = []
	exports = []
	cc = country_code if country_code!=china_producing_code else china_trade_code
	for item_code in source_codes:
		i,e = 0.0,0.0
		spec = {'year':year,'countrycode':cc,'itemcode':item_code}
		fields = {'elementcode':1,'value':1}
		qry,flag = find(table_tradecropslivestock,spec,fields)
		#qry = db.tradecropslivestock.find({'year':year,'countrycode':country_code,'itemcode':item_code},{'elementcode':1,'value':1})
		for rec in qry:
			if rec['elementcode'] in import_codes:
				i += rec['value']
			elif rec['elementcode'] in export_codes:
				e += rec['value']
		
		if item_code in livestockprimary_codes+livestock_codes:
			if item_code in livestockprimary_codes:
				item_code = livestock_mappings[item_code]
			
			carcass_weight = get_carcass_weight(year,country_code,item_code)
			spec = {'year':year,'countrycode':cc,'itemcode':item_code}
			qry,flag = find(table_tradeliveanimals,spec,fields)
			for rec in qry:
				if rec['elementcode'] in import_codes:
					i += carcass_weight*rec['value']
				elif rec['elementcode'] in export_codes:
					e += carcass_weight*rec['value']
		imports.append(i)
		exports.append(e)
			
	return imports,exports
	
def get_production(year,country_code,item_code,sys_code=-5511,imports=True,exports=True,cull=True,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Get the production quantity for the given primary item code (see get_source_codes)
	or live animal code.  See get_yield for more details.
		-'year' is the year for which to get the yield.
		-'country_code' specifies the country for which to get the yield.
		-'item_code' specifies the primary item whose yield is to be calculated.
		-'get_next' specifies whether to get the next available record if
		  none exists for the specified year.
		-'aggregate' specifies whether to average over the country's (sub-)continent
		  if none exists for the specified country.
		  
	Note:  Country mappings are automatically applied.
	"""
	if country_code in country_mappings:
		country_code = country_mappings[country_code]
		#flag = ....
	if item_code in crop_codes:
		return get_crop_production(year,country_code,item_code,from_db=from_db)
		
	elif item_code in livestockprimary_codes:
		return get_livestockprimary_production(year,country_code,item_code,imports,exports,cull,from_db=from_db)
		
	elif item_code in livestock_codes:
		item_code = livestock_reverse_mappings[item_code]
		return get_livestockprimary_production(year,country_code,item_code,imports,exports,cull,from_db=from_db)
	else:
		raise ValueError
	
def get_carcass_weight(year, country_code, item_code, get_next=True, aggregate=True):
	"""
	Given a year, country code and live animal code (item_code), return the carcass weight
	in tonnes of that animal.
		-'year' is the year for which to get the carcass weight.
		-'country_code' specifies the country for which to get the carcass weight.
		-'item_code' specifies the primary item whose carcass weight is to be calculated.
		-'get_next' specifies whether to get the next available record if
		  none exists for the specified year.
		-'aggregate' specifies whether to average over the country's (sub-)continent
		  if none exists for the specified country.
	"""
	if country_code in country_mappings:
		country_code = country_mappings[country_code]
	lp_code = livestock_reverse_mappings[item_code]
	spec = {'year':year,'countrycode':country_code,'itemcode':lp_code,'elementcode':{'$in':carcass_codes}}
	fields = {'elementcode':1,'value':1}
	rec,flag = find_one(table_productionlivestockprimary,spec,fields,get_next=True,aggregate='countrycode')
	if rec is not None:
		conv = Hg2tonnes if rec['elementcode'] == 5417 else dg2tonnes
		carcass_weight = conv*rec['value']
	else:
		carcass_weight = 0.0
	return carcass_weight
	
def get_fraction_of_ag(year,country_code,item_code,sector="total",flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given year, country code, and crop item code, return the yield.
	"""
	if from_db:
		spec = {'year':year,'countrycode':country_code}
		fields = {'value':1,'flag':1}
		rec = table_agrilandfraction.find_one(spec,fields)
		(y,flag) = (rec['value'],rec['flag']) if rec is not None else (float('inf'),"No data")
		return y,flag
	
	agri_land,al_flag = get_agricultural_area(year,country_code,sector=sector,from_db=True)
	area_harvested,ah_flag = get_area_harvested(year,country_code,item_code,sector)
	if item_code in livestockprimary_codes:
		try:
			area_harvested = area_harvested[sector] if area_harvested is not None else 0.0
		except TypeError:
			print year,country_code,item_code,area_harvested
			raise
	
	#print year,country_code,item_code,agri_land
	
	frac = area_harvested/float(agri_land) if area_harvested is not None and agri_land!=0.0 else 0.0
	flag = ''
	
	return frac,flag
	
def get_agricultural_area(year,country_code,sector="total",flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given year, country code, and crop item code, return the yield.
	"""
	
	if from_db:
		if sector=="total":
			table = table_agriland
		elif sector=="crop":
			table = table_cropland
		spec = {'year':year,'countrycode':country_code}
		fields = {'value':1,'flag':1}
		rec = table.find_one(spec,fields)
		(y,flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		return y,flag
		
	if sector=="total":
		item_code = agricultural_land_code
	elif sector=="crop":
		item_code = {'$in':cropland_codes}
	
	if org_year is None:
		org_year = year
		
	fields = {'value':1}
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':area_code}
	qry,f = find(table_land, spec, fields)
	y=0.0
	for rec in qry:
		y += 1000.0*rec['value'] if rec is not None else 0.0
	
	no_harvest = y==0.0
	
	if no_harvest:
		spec = {'year':{'$gt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':area_code}
		rec,f = find_one(table_land, spec, fields, sort=[('year',1)])
		y = 1000.0*rec['value'] if rec is not None else 0.0
		#flag = "Ny"
		
	no_harvest = y==0.0
	
	if no_harvest:
		spec = {'year':{'$lt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':area_code}
		rec,f = find_one(table_land, spec, fields, sort=[('year',1)])
		y = 1000.0*rec['value'] if rec is not None else 0.0
		#flag = "Py"
		
	no_harvest = y==0.0
	
	if no_harvest:
		y = 0.0
		flag = ["No data"]
	
	#print flag
	flag = ''.join(flag)
	return y,flag
	
def get_crop_yield(year,country_code,item_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given year, country code, and crop item code, return the yield.
	"""
	if from_db:
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code}
		fields = {'value':1,'flag':1}
		rec,f = find_one(table_cropyields,spec,fields)
		(y,flag) = (rec['value'],rec['flag']) if rec is not None else (float('inf'),"No data")
		return y,flag
	
	if org_year is None:
		org_year = year
		
	fields = {'value':1}
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':yield_code}
	rec,f = find_one(table_productioncrops, spec, fields)
	y = Hg2tonnes*rec['value'] if rec is not None else None
	
	no_harvest = y is None
	
	if no_harvest:
		spec = {'year':{'$gt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':yield_code}
		rec,f = find_one(table_productioncrops, spec, fields, sort=[('year',1)])
		y = Hg2tonnes*rec['value'] if rec is not None else None
		#flag = "Ny"
		
	no_harvest = y is None
	
	if no_harvest:
		spec = {'year':{'$lt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':yield_code}
		rec,f = find_one(table_productioncrops, spec, fields, sort=[('year',1)])
		y = Hg2tonnes*rec['value'] if rec is not None else None
		#flag = "Py"
		
	no_harvest = y is None
	
	if no_harvest:
		region_code = get_country_region(country_code,1)
		spec = {'year':{'$lt':year},'countrycode':region_code,'itemcode':item_code,'elementcode':yield_code}
		rec,f = find_one(table_productioncrops, spec, fields)
		y = Hg2tonnes*rec['value'] if rec is not None else None
		#flag = "A"+str(region_code)
	
	no_harvest = y is None
	
	if no_harvest:
		spec = {'year':{'$lt':year},'countrycode':world_code,'itemcode':item_code,'elementcode':yield_code}
		rec,f = find_one(table_productioncrops, spec, fields)
		y = Hg2tonnes*rec['value'] if rec is not None else None
		#flag = "A"+str(world_code)
		
	no_harvest = y is None
	
	if no_harvest:
		y = 0.0
		flag = "No data"
		
	#flag = ''.join(flag)
	flag = ''
	return y,flag
	
def get_crop_area_harvested(year,country_code,item_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given year, country code, and crop item code, return the area harvested
	
	To do : create the db table
	"""
	
	if from_db:
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code}
		fields = {'value':1,'flag':1}
		rec = table_cropareaharvested.find_one(spec,fields)
		(a,flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		return a,flag
	
	if org_year is None:
		org_year = year
		
	fields = {'value':1}
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':area_harvested_code}
	rec,f = find_one(table_productioncrops, spec, fields)
	a = rec['value'] if rec is not None else None
	
	no_harvest = a is None
	
	if no_harvest:
		spec = {'year':{'$gt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':area_harvested_code}
		rec,f = find_one(table_productioncrops, spec, fields, sort=[('year',1)])
		a = rec['value'] if rec is not None else None
		#flag = "Ny"
		
	no_harvest = a is None
	
	if no_harvest:
		spec = {'year':{'$lt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':area_harvested_code}
		rec,f = find_one(table_productioncrops, spec, fields, sort=[('year',1)])
		a = rec['value'] if rec is not None else None
		#flag = "Py"
		
	no_harvest = a is None
	
	if no_harvest:
		a = 0.0
		flag = "No data"
		
	#flag = ''.join(flag)
	flag = ''
	return a,flag
	
def get_crop_production(year,country_code,item_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given year, country code, and crop item code, return the yield.
	"""
	if from_db:
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code}
		fields = {'value':1,'flag':1}
		rec = table_cropproduction.find_one(spec,fields)
		(p,flag) = (rec['value'],rec['flag']) if rec is not None else (float('inf'),"No data")
		return p,flag
	
	if org_year is None:
		org_year = year
		
	fields = {'value':1}
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':production_code}
	rec,f = find_one(table_productioncrops, spec, fields)
	p = rec['value'] if rec is not None else None
	
	no_harvest = p is None
	
	if no_harvest:
		spec = {'year':{'$gt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':production_code}
		rec,f = find_one(table_productioncrops, spec, fields, sort=[('year',1)])
		p = rec['value'] if rec is not None else None
		#flag = "Ny"
		
	no_harvest = p is None
	
	if no_harvest:
		spec = {'year':{'$lt':year},'countrycode':country_code,'itemcode':item_code,'elementcode':production_code}
		rec,f = find_one(table_productioncrops, spec, fields, sort=[('year',1)])
		p = rec['value'] if rec is not None else None
		#flag = "Py"		
		
	no_harvest = p is None
	
	if no_harvest:
		p = 0.0
		flag = "No data"
		
	flag = ''
	return p,flag
	
def get_feed_conversion(year,country_code,lp_code,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=False):
	"""
	Given year, country code and primary livestock item code, return the feed conversion
	(i.e. (feed)/(livestock product))
	"""
	if from_db:
		spec = {'year':year,'countrycode':country_code,'itemcode':lp_code}
		fields = {'value':1,'flag':1}
		rec,f = find_one(table_feedconversion,spec,fields)
		(fc,flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		return fc,flag
	
	if org_year is None:
		org_year = year
		
	flag = ''
	
	feed_quantities,fq_flag = get_feed_quantities(year,country_code,lp_code,domestic=False)
	#if fq_flag!='':
	#	flag += "Fq"+fq_flag+"Fq"
	feed_quantity = sum(feed_quantities.values())
	##meat_production,mp_flag = get_livestockprimary_production(year,country_code)
	##meat_production = meat_production['ML'][item_code]
	meat_production,mp_flag = get_livestockprimary_production(year,country_code,lp_code)#,sys_code=-5512)
	meat_production = meat_production['ML'] #only production in mixed/landless system uses feed.
	#if mp_flag!='':
	#	flag += "Mp"+mp_flag+"Mp"
	
	no_data = feed_quantity==0 or meat_production==0
	if no_data and next_dir>-1 and year<max_year:  #get next
		next_dir = 1
		#flag.append("Ny")
		return get_feed_conversion(year+1,country_code,lp_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and year==max_year and org_year!=min_year:
		next_dir = -1
		#flag = list(set(flag)-set(['Ny']))#flag.translate(None,'Ny')+"Py"
		#flag.append('Py')
		return get_feed_conversion(org_year-1,country_code,lp_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append('Py')
		return get_feed_conversion(year-1,country_code,lp_code,flag,org_year,next_dir,aggregate_level)
	elif no_data and country_code!=world_code:
		#flag = list(set(flag)-set(['Py'])-set(['Ny']))#flag.translate(None,'Py').translate(None,'Ny')
		aggregate_level+=1
		region_code = get_country_region(country_code,aggregate_level)
		#flag.extend(['A',str(region_code)])#'A'+str(region_code)
		return get_feed_conversion(org_year,region_code,lp_code,flag,org_year,next_dir,aggregate_level)
	elif no_data:
		feed_conversion = {k:0.0 for k,v in feed_quantities.iteritems()}
		feed_conversion["total"] = 0.0
		return feed_conversion,"No data"
		
	feed_conversion = {k:v/meat_production for k,v in feed_quantities.iteritems()}
	feed_conversion["total"] = feed_quantity/meat_production
	
	flag = ''
	return feed_conversion,flag
	
def get_livestockprimary_area_harvested(year,country_code,item_code,sector="total",flag=[],org_year=None,next_dir=0,aggregate_level=0,get_next=False,from_db=False):
	"""
	Given year, country code, and primary livestock item code, return the area harvested
	"""
	if from_db:
		(lpah,lpa_flag) = ({"T":0.0,"P":0.0,"P_ML":0.0,"P_P":0.0,"C":0.0},"No data")
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code}#,'elementcode':sys_code}
		fields = {'elementcode':1,'value':1,'flag':1}
		qry,f = find(table_livestockareaharvested,spec,fields)
		for rec in qry:
			if rec['elementcode']==-5313:
				lpah['T']=rec['value']
			elif rec['elementcode']==-5314:
				lpah['C']=rec['value']
			elif rec['elementcode']==-5315:
				lpah['P']=rec['value']
			elif rec['elementcode']==-5316:
				lpah['P_ML']=rec['value']
			elif rec['elementcode']==-5317:
				lpah['P_P']=rec['value']
			else:
				print "Invalid elementcode in livestockareaharvested"
				raise ValueError
		lpa_flag = ''
		return lpah,lpa_flag
	"""if from_db:
		if sector == "total":
			sys_code = -5313
		elif sector == "crop":
			sys_code = -5315
		elif sector == "pasture":
			sys_code = -5314
		spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':sys_code}
		fields = {'value':1,'flag':1}
		rec,f = find_one(table_livestockareaharvested,spec,fields)
		(lpa_production,lpa_flag) = (rec['value'],rec['flag']) if rec is not None else (0.0,"No data")
		return lpa_production,lpa_flag
	"""
	if org_year is None:
		org_year = year
		
	#if item_code in meat_animal_mappings.keys():
	#	animal_code = meat_animal_mappings[item_code]
	#elif item_code in milkeggs_animal_mappings.keys():
	#	animal_code = item_code
	#else:
	#	print "Itemcode",item_code,"is not a valid code"
	#	raise ValueError
	
	pasture_area = 0.0
	pasture_area_ML = 0.0
	pasture_area_P = 0.0
	if item_code in items_that_use_pasture:
		(pa, pa_flag) = get_pasture_areas(year,country_code,item_code)
		pasture_area = pa['T']		#total
		pasture_area_ML = pa['ML']	#mixed/landless
		pasture_area_P = pa['P']	#pastoral
	
	
	
	
	#print pasture_area,animal_code
	
	#if item_code in items_that_use_pasture:
	#	spec = {'year':year,'countrycode':country_code,'itemcode':animal_code}
	#	fields = {'value':1}
	#	rec,f = find_one(table_pastureareas,spec,fields)
	#	pasture_area = rec['value']
		#pa_flag = rec['flag']
		#print pa_flag
		#if pa_flag!='':
		#	flag.extend(["Pa",pa_flag,"Pa"])
		
	#print pasture_area,animal_code
		
	feed_quantities,fq_flag = get_feed_quantities(year,country_code,item_code)
	#if fq_flag!='':
	#	flag.extend(["Fq",fq_flag,"Fq"])
	crop_area = 0.0
	#for crop_code,quantity in feed_quantities.iteritems():
	#	yld,y_flag = get_crop_yield(year,country_code,crop_code,from_db=True)
	#	print quantity,yld
		#if y_flag!='':
		#	flag.extend(["Y",str(crop_code),y_flag,"Y"])
	#	crop_area += quantity/yld if yld!=0 else 0.0
	
	#Get crop yields in one shot.  This is faster than using get_crop_yield
	spec = {'year':year,'countrycode':country_code,'itemcode':{'$in':feed_quantities.keys()}}
	fields = {'itemcode':1,'value':1}
	qry,f = find(table_cropyields,spec,fields)
	for rec in qry:
		yld = rec['value']
		quantity = feed_quantities[rec['itemcode']]
		crop_area += quantity/yld if yld!=0 else 0.0
		
	no_harvest = (pasture_area+crop_area)==0
	if get_next and no_harvest and next_dir>-1 and year<max_year-1:  #the -1 is because land areas aren't available yet for 2010
		next_dir = 1
		#flag.append("Ny")
		return get_livestockprimary_area_harvested(year+1,country_code,item_code,sector,flag,org_year,next_dir,aggregate_level,get_next)
	elif get_next and no_harvest and year==max_year-1 and org_year!=min_year: #the -1 is because land areas aren't available yet for 2010
		next_dir = -1
		#flag = list(set(flag)-set(['Ny']))#flag.translate(None,'Ny')+"Py"
		#flag.append('Py')
		return get_livestockprimary_area_harvested(org_year-1,country_code,item_code,sector,flag,org_year,next_dir,aggregate_level,get_next)
	elif get_next and no_harvest and next_dir < 0 and year>min_year:
		next_dir = -1
		#flag.append("Py")
		return get_livestockprimary_area_harvested(year-1,country_code,item_code,sector,flag,org_year,next_dir,aggregate_level,get_next)
	elif no_harvest:
		flag = ["No data"]
		a = None
		
	areas_harvested = {"T":pasture_area+crop_area,"P":pasture_area,"P_ML":pasture_area_ML,"P_P":pasture_area_P,"C":crop_area}
	flag = ''.join(flag)
	#flag = '' #flag routine is buggered if get_next=True
	return areas_harvested,flag

def get_import_export(year,country_code,item_code,flag=[]):
	"""
	Get quantities of item_code imported and exported by the given country in the given year.
	
	Note: This function does not yet work on live animal codes.
	"""
	(imports,exports) = (0.0,0.0)
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':{'$in':import_codes+export_codes}}
	fields = {'elementcode':1,'value':1}
	qry,f = find(table_tradecropslivestock,spec,fields)
	
	for rec in qry:
		if rec['elementcode'] in import_codes:
			imports = rec['value']
		elif rec['elementcode'] in export_codes:
			exports = rec['value']
		else:
			print rec['elementcode']
			raise ValueError
	
	return imports,exports
	

def get_area_harvested(year,country_code,item_code,sector="total",flag=[],org_year=None,next_dir=0,aggregate_level=0,get_next=False,from_db=False):
	"""
	Get the land area harvested for the given primary item code (see get_source_codes) or live animal code.
	For primary livestock items, the area is given by the sum of the pasture area associated with the livestock animal
	(see get_pasture_areas) and the crop area associated with feed (see get_feed_crop_area)
	For live animals (livestock), the item code is mapped to the corresponding animal
	carcass code (e.g. Cattle (866) -> Cattle meat (867), Chicken (1057) -> Chicken meat (1058)...)
	then treated as a primary livestock (meat) item.
		-'year' is the year for which to get the yield.
		-'country_code' specifies the country for which to get the yield.
		-'item_code' specifies the primary item whose yield is to be calculated.
		-'get_next' specifies whether to get the next available record if
		  none exists for the specified year.
		-'aggregate' specifies whether to average over the country's (sub-)continent
		  if none exists for the specified country.
		-'pasture_areas' can be pre-calculated using get_pasture_areas, or if None
		  is provided it will be calculated here.
		  
	Note:  It may be necesarry to get country mappings for the country_code before calling this function.
	Note:  Return value is in units of Ha
	"""
	
	ret_flag = ''
	fields = {'value':1}
	if item_code in crop_codes:
		return get_crop_area_harvested(year,country_code,item_code,from_db=from_db)
	
	elif item_code in livestockprimary_codes:
		return get_livestockprimary_area_harvested(year,country_code,item_code,sector,from_db=from_db)
		
	elif item_code in livestock_codes:
		item_code = livestock_reverse_mappings[item_code]
		return get_livestockprimary_area_harvested(year,country_code,item_code,sector,from_db=from_db)
	else:
		raise ValueError
	
	return a,flag


def fetch_yield(year,country_code,item_code,pasture_as_feed=True):
	"""
	Get the yield for the given primary item code (see get_source_codes) or live animal code
	or compound feed code.  This value is fetched directly from a database table produced by
	iterating get_yield() over all included itemcodes.
		  
	Note:  It may be necesarry to get country mappings for the country_code before calling this function.
	"""
	spec = {'year':year,'countrycode':country_code,'itemcode':item_code,'elementcode':yield_code}
	fields = {'value':1,'flag':1}
	rec,flag = find_one(table_yields, spec, fields)
	if rec is None:
		return 0.0,"no data"
	
	return rec['value'],rec['flag']
	
def get_yield(year,country_code,item_code,sector="total",imports=True,exports=True,cull=False,flag=[],org_year=None,next_dir=0,aggregate_level=0,from_db=True):
	"""
	Get the yield for the given primary item code (see get_source_codes) or live animal code.
	For primary livestock items, the yield is calculated as the production
	divided by the sum of the pasture area associated with the livestock animal
	(see get_pasture_areas) and the crop area associated with feed (see get_feed_crop_area)
	For live animals (livestock), the item code is mapped to the corresponding animal
	carcass code (e.g. Cattle (866) -> Cattle meat (867), Chicken (1057) -> Chicken meat (1058)...)
	then treated as a primary livestock (meat) item.
		-'year' is the year for which to get the yield.
		-'country_code' specifies the country for which to get the yield.
		-'item_code' specifies the primary item whose yield is to be calculated.
		-'get_next' specifies whether to get the next available record if
		  none exists for the specified year.
		-'aggregate' specifies whether to average over the country's (sub-)continent
		  if none exists for the specified country.
		-'pasture_areas' can be pre-calculated using get_pasture_areas, or if None
		  is provided it will be calculated here.
		-'pasture_mode' specifies how to compute the "harvested area" for livestock products.
		  It may be either 'feed' or 'stock' (see get_area_harvested).
		  
	Note:  Country mappings are automatically applied.
	"""
	ret_flag = ''
	if country_code in country_mappings:
		country_code = country_mappings[country_code]
		
	if item_code in crop_codes:
		return get_crop_yield(year,country_code,item_code,from_db=from_db)
	
	elif item_code in livestockprimary_codes:
		return get_livestockprimary_yield(year,country_code,item_code,imports=imports,exports=exports,cull=cull,from_db=from_db)
		
	elif item_code in livestock_codes:
		item_code = livestock_reverse_mappings[item_code]
		return get_livestockprimary_yield(year,country_code,item_code,imports=imports,exports=exports,cull=cull,from_db=from_db)
	else:
		print "Invalid item code",item_code,"for get_yield()"
		raise ValueError
	
def get_all_countries(struct = 'list'):
	"""
	Returns either a dictionary or list of all countries.
	"""
	collection = db.countries
	return get_countries(collection,struct)

def get_producing_countries(struct = 'list'):
	"""
	Returns either a dictionary or list of countries listed as producers.
	"""
	collection = db.producers
	return get_countries(collection,struct)
	
def get_trade_reporter_countries(struct = 'list'):
	"""
	Returns either a dictionary or list of all countries that report trade.
	"""
	collection = db.reporters
	return get_countries(collection,struct)
	
def get_trade_partner_countries(struct = 'list'):
	"""
	Returns either a dictionary or list of all countries that are trade partners.
	"""
	collection = db.partners
	return get_countries(collection,struct)

def get_balancing_countries(struct = 'list'):
	"""
	Returns either a dictionary or list of all countries that report commodity balances.
	"""
	collection = db.balancers
	return get_countries(collection,struct)

def get_countries(collection,struct):
	"""
	Returns either a dictionary of countrycode:country pairs (if struct='dict'),
	or a dictionary of country:countrycode pairs (if struct='dict2'),
	or a list of countrycodes (if struct='list'),
	or a list of countries (if struct='list2'),
	"""
	if struct=='list':
		return [rec['countrycode'] for rec in collection.find()]
	elif struct=='dict':
		return {rec['countrycode']:rec['country'] for rec in collection.find()}
	elif struct=='list2':
		return [rec['country'] for rec in collection.find()]
	elif struct=='dict2':
		return {rec['country']:rec['countrycode'] for rec in collection.find()}
	else:
		raise ValueError
	
def get_country_region(country_code,level=1):
	"""
	Fetch the sub-continent or continent for the given country code.  
	Returns the aggregate (region) code corresponding to the smallest
	geographical unit (i.e. largest aggregate code).  
	
	level can be 1 (sub-continent) or 2 (continent) or 3 (world)
	"""
	if level==3:
		return world_code
	if country_code in country_mappings:
		country_code = country_mappings[country_code]
	if country_code >= world_code:
		return country_code	
	qry = db.countryaggregates.find({'aggregatecode':{'$lt':5600},'countrycode':country_code},{'aggregatecode':1},sort=[('aggregatecode',-1)])
	if level==2:
		qry.next()
	try:
		region_code = qry.next()['aggregatecode']
	except StopIteration:
		print country_code
		raise StopIteration
	
	#aggregate_code = db.countryaggregates.find_one({'aggregatecode':{'$lt':5600},'countrycode':country_code},{'aggregatecode':1},sort=[('aggregatecode',-1)])['aggregatecode'] if country_code < world_code else country_code #country codes >= 5000 are already aggregate codes
	return region_code
	
def get_country_mappings():
	"""
	Returns a dictionary of key:value pairs that map special countries (usually trade
	partners that aren't also producers) to associated countries or regions.
	(e.g. China, mainland (41) gets mapped to China (351)).
	"""
	return {mapping['fromcode']:mapping['tocode'] for mapping in db.countrymappings.find()}
	
def get_crop_codes():
	"""
	Returns a list of item codes for all crops produced.
	"""
	return [rec['itemcode'] for rec in db.cropsproduced.find()]
	
def get_livestockprimary_codes():
	"""
	Returns a list of item codes for all primary livestock commodities.
	"""
	return [rec['itemcode'] for rec in db.livestockprimaryproduced.find()]
	
def get_livestock_codes():
	"""
	Returns a list of item codes for all primary livestock commodities.
	"""
	return [rec['itemcode'] for rec in db.liveanimalsproduced.find()]
	
def get_livestock_mappings():
	"""
	Returns a dictionary of key:value pairs where key is a primary livestock itemcode
	and value is the corresponding live animal code.
	"""
	return {mapping['fromcode']:mapping['tocode'] for mapping in db.livestockmappings.find()}
	
def find(collection, spec, fields=None, sort=None, aggregate=None, flag=''):
	"""
	Fetch cursor to lfaodb records.
		- 'collection' is the mongo collection to query (e.g. db.productioncrops)
		- 'spec' is a dictionary of conditions to match against (e.g. {'year':2001, 'countrycode':231, 'itemcode':56})
		- 'fields' is a dictionary of fields to fetch (e.g. {'elementcode':1,'value':1})
		- 'sort' is a list of tuples (field, order) used to sort the query (e.g. [('year',1)]).  order is 1=ascending or -1=descending)
		- 'aggregate' is the key corresponding to the "country"code over which to aggregate
		   if the cursor comes up empty.  Typically one of 'countrycode', 'reportercode', or 'partnercode'
		
	If aggregate is not None and the initial query comes up empty, the db is re-queried on the
	specified country's (sub-)continent.
	"""
	qry = collection.find(spec,fields=fields,sort=sort)
	try:
		qry.next()
		qry.rewind()
	except StopIteration:
		#qry = None
		if aggregate is not None and spec[aggregate]<world_code: #5000 is where geo-aggregate codes begin.
			region_code = get_country_region(spec[aggregate])
			spec[aggregate] = region_code
			flag=flag+'a'
			return find(collection,spec,fields,sort,None,flag)
	return qry,flag

def find_one(collection, spec, fields=None, sort=[], get_next=False, aggregate=None, flag=''):
	"""
	Fetch one to lfaodb record.
		- 'collection' is the mongo collection to query (e.g. db.productioncrops)
		- 'spec' is a dictionary of conditions to match against (e.g. {'year':2001, 'countrycode':231, 'itemcode':56})
		- 'fields' is a dictionary of fields to fetch (e.g. {'elementcode':1,'value':1})
		- 'sort' is a list of tuples (field, order) used to sort the query (e.g. [('year',1)]).  order is 1=ascending or -1=descending)
		- 'get_next' is a tuple (field,dir).  If no record is found, then the next available record is fetched from a cursor
		   sorted by field in the direction dir (e.g. ('year','$gt') ). 
		- 'aggregate' is the field corresponding to the "country"code over which to aggregate
		   if the cursor comes up empty.  Typically one of 'countrycode', 'reportercode', or 'partnercode'
		
	If get_next is not None and the initial query comes up empty, then attempt is made to get the next available
	record in the direction specified by get_next_order.  This takes precedence over aggregate.
	
	If aggregate is not None and the initial query comes up empty, the db is re-queried on the
	specified country's (sub-)continent.
	
	Returns None if empty query is ultimately fetched
	"""
	rec = collection.find_one(spec,fields=fields,sort=sort)
	if rec is None and get_next:
		try:
			spec['year'] = {'$gt':spec['year']}
			sort = sort + [('year', 1)]
			qry,f = find(collection, spec, fields, sort, aggregate)
			rec = qry.next()
			flag = flag+f+'n'
		except StopIteration:
			try:
				spec['year'] = {'$lt':spec['year']['$gt']}
				sort = sort + [('year', -1)]
				qry,f = find(collection, spec, fields, sort, aggregate)
				rec = qry.next()
				flag = flag+f+'p'
			except StopIteration:
				rec = None
				if aggregate is not None and spec[aggregate]<world_code: #5000 is where geo-aggregate codes begin.
					region_code = get_country_region(spec[aggregate],0)
					spec[aggregate] = region_code
					flag = flag+'a'
					rec = find_one(collection,spec,fields,sort,get_next,None,flag)
					if rec is None:
						region_code = get_country_region(spec[aggregate],1)
						spec[aggregate] = region_code
						flag = flag+'a'
						rec = find_one(collection,spec,fields,sort,get_next,None,flag)
						if rec is None:
							spec[aggregate] = world_code
							flag = flag+'a'
							rec = find_one(collection,spec,fields,sort,get_next,None,flag) 
					
	return rec,flag
			
#def find_one(collection, spec, fields=None, sort=[], get_next=None, aggregate=None, flag=''):
	"""
	Fetch one to lfaodb record.
		- 'collection' is the mongo collection to query (e.g. db.productioncrops)
		- 'spec' is a dictionary of conditions to match against (e.g. {'year':2001, 'countrycode':231, 'itemcode':56})
		- 'fields' is a dictionary of fields to fetch (e.g. {'elementcode':1,'value':1})
		- 'sort' is a list of tuples (field, order) used to sort the query (e.g. [('year',1)]).  order is 1=ascending or -1=descending)
		- 'get_next' is a tuple (field,dir).  If no record is found, then the next available record is fetched from a cursor
		   sorted by field in the direction dir (e.g. ('year','$gt') ). 
		- 'aggregate' is the field corresponding to the "country"code over which to aggregate
		   if the cursor comes up empty.  Typically one of 'countrycode', 'reportercode', or 'partnercode'
		
	If get_next is not None and the initial query comes up empty, then attempt is made to get the next available
	record in the direction specified by get_next_order.  This takes precedence over aggregate.
	
	If aggregate is not None and the initial query comes up empty, the db is re-queried on the
	specified country's (sub-)continent.
	
	Returns None if empty query is ultimately fetched
	"""
#	rec = collection.find_one(spec,fields=fields,sort=sort)
#	if rec is None and get_next is not None:
#		spec[get_next[0]] = {get_next[1]:spec[get_next[0]]}
#		order = 1 if get_next[1]=='$gt' else -1
#		sort = sort + [(get_next[0], order)]
#		qry,f = find(collection, spec, fields, sort, aggregate)
#		try:
#			rec = qry.next()
#			flag = flag+f+'n'
#		except StopIteration:
#			rec = None
#			if aggregate is not None and spec[aggregate]<world_code: #5000 is where geo-aggregate codes begin.
#				region_code = get_country_region(spec[aggregate])
#				spec[aggregate] = region_code
#				flag = flag+'a'
#				return find_one(collection,spec,fields,sort,get_next,None,flag)
#	return rec,flag

def find_sum(collection,spec,field,get_next=False,reverse=False,group='year',flag=''):
	"""
	Sum over the fields of a query
		- 'collection' is the collection to query on.
		- 'spec' is a dictionary of conditions to match.
		- 'field' the field to sum over (e.g. 'value' (note the $))
		- 'get_next' specifies if you want to fetch the next available record if
		  no match is found (i.e. next available according to group).
		- 'reverse': If True then reverse the sense of get_next.
		- 'group' is a field to group by (e.g. 'year' (note the $))
		
	Returns 0 if no match is found.
	"""
	result = collection.aggregate([{'$match':spec},{'$group':{'_id':'$'+group,'total':{'$sum':'$'+field}}}])['result']
	result = sorted(result,key=lambda k: k['_id'],reverse=reverse)
	try:
		s = result[0]['total']
	except IndexError:
		s = 0
		if get_next:
			get_next_dir = '$gt' if not reverse else '$lt'
			spec[group] = {get_next_dir:spec[group]}
			flag = flag+'n'
			return find_sum(collection, spec, field, False, reverse, group,flag)
	return s,flag
	


#Database connection	
connection = Connection()
db = connection.lfaodb
#Database collection objects
table_productioncrops = db.productioncrops
table_productioncropsprocessed = db.productioncropsprocessed
table_productionlivestock = db.productionlivestock
table_productionlivestockprimary = db.productionlivestockprimary
table_livestockproductionnoadj = db.livestockproductionnoadj
table_livestockproductionexport = db.livestockproductionexport
table_livestockproductionimportexport = db.livestockproductionimportexport
table_livestockproductionimportexportcull = db.livestockproductionimportexportcull
table_liveanimalproduction = db.liveanimalproduction
table_productionlivestockprocessed = db.productionlivestockprocessed
table_tradecropslivestock = db.tradecropslivestock
table_tradeliveanimals = db.tradeliveanimals
table_tradematrix = db.tradematrix
table_commoditybalance = db.commoditybalance
table_foodbalance = db.foodbalancesheets
table_cropsproduced = db.cropsproduced
table_livestockproduced = db.livestockproduced
table_livestockprimaryproduced = db.livestockprimaryproduced
table_livestockareaharvested = db.livestockareaharvested
table_livestockyields = db.livestockyieldsimportexport
table_countries = db.countries
table_countrymappings = db.countrymappings
table_producers = db.producers
table_reporters = db.reporters
table_partners = db.partners
table_balancers = db.balancers
table_livestockmappings = db.livestockmappings
table_livestockunits = db.livestockunits
table_cullrates = db.cullrates
table_commoditytrees = db.commoditytrees
table_feedtodomesticratio = db.feedtodomestic
table_feedmixes = db.feedmixes
table_feedfoodfractions = db.feedfoodfractions
table_feedshares = db.feedshares
table_feedconversion = db.feedconversion
table_feedssr = db.feedssr
table_ssr = db.ssr
table_feedconversionparams = db.feedconversionparams
table_systemproductionfractions = db.systemproductionfractions
table_systemanimalfractions = db.systemanimalfractions
table_systemslaughterfractions = db.systemslaughterfractions
table_systemareafractions = db.systemareafractions
table_land = db.land
table_agriland = db.agriland
table_cropland = db.cropland
table_agrilandfraction = db.agrilandfraction
table_pastureareas = db.pastureareas
table_population = db.population
table_cropyields = db.cropyields
table_cropproduction= db.cropproduction
table_cropareaharvested = db.cropareaharvested

#Other constants
min_year = 1961
max_year = 2010
export_group = 91
export_codes = [5910,5909,5908,5907]
export_code = 5910 # this one is for quantitiy in tonnes
export_code = 5911 # this one is for quantitiy in 1000 tonnes (foodbalancesheets)
import_group = 61
import_codes = [5610,5609,5608,5607]
import_code = 5610 # this one is for quantitiy in tonnes
import_code_fb = 5611 # this one is for quantitiy in 1000 tonnes (foodbalancesheets)
yield_code = 5419
production_code = 5510
production_code_fb = 5511
carcass_codes = [5417,5424]
Hg2tonnes = 0.0001
dg2tonnes = 0.0000001
byproduct_codes = [1,3]

balance_reporters = get_balancing_countries()
cereal_code_balance = 2905 #in commoditybalance table
cereal_code_production = 1717 #in production table
feed_code = 5520 #feed element in commoditybalance table
feed_code_fb = 5521 #feed element in commoditybalance table
food_supply_code_fb = 664 #food supply (kcal/capita) element in foodbalancesheets table
food_code = 5141 #food element in commoditybalance table
food_code_fb = 5142 #food element in foodbalance table
production_code_balance = 5511 #production element in commoditybalance table
import_code_balance = 5611 #import element in commoditybalance table
domestic_supply_code = 5300
domestic_supply_code_fb = 5301
population_item_code = 3010 
population_element_code = 511

agricultural_land_code = 6610
cropland_codes = [6650,6621]
pasture_codes = [6655, 6633] #temporary and permanent pastures
area_code = 5110  #or is it 5312???
area_harvested_code = 5312
#milk_codes = [951,882,1020,982,1130,1062,1091]#,987 #fresh milk and eggs codes in productionlivestockprimary
#milk_animal_codes = [946,866,1016,976,1126,1057,1083]#,976 #milk-or-egg-producing animal codes in productionlivestock
#milk_animal_number_codes = [5318,5313] #code for the number of animals producing milk

cattle_codes = [867]

livestock_mappings = get_livestock_mappings()
livestock_reverse_mappings = {y:x for x,y in livestock_mappings.iteritems()}
feed_categories = { #keys are food groups, first tuple entry is list of included itemcodes from commiditybalance, second tuple entry is list of corresponding itemcodes from productioncrops, third tuple entry (if present) is conversion factor to primary crop.
	"cereal":([2511,2804,2513,2514,2515,2516,2517,2518,2520],
			  [15,27,44,56,71,75,79,83,108],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]),
	"roots":([2531,2532,2533,2534,2535],[116,125,122,149,137],[1.0,1.0,1.0,1.0,1.0]),
	"sugarcrops":([2536,2537],[156,157],[1.0,1.0]),
	"sugar":([2827],[156],[0.11]),
	"pulses":([2546,2547,2549],[176,187,191],[1.0,1.0,1.0]),
	"nuts":([2551],[1729],[1.0]),
	"oilcrops":([2555,2820,2557,2558,2559,2560,2561,2563,2570],[236,242,267,270,328,249,289,260,339],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0]),
	"oil":([2571,2572,2573,2574,2575,2576,2578,2579,2586],[236,242,267,270,328,254,249,289,339],[0.18,0.30,0.41,0.38,0.10,0.19,0.13,0.43,0.3]),
	"fruitnveg":([2601,2602,2605,2611,2612,2613,2614,2615,2616,2618,2619,2620,2625],[388,403,358,490,497,507,512,486,489,574,577,560,619],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])}
feed_items_in_balance = feed_categories["cereal"][0]+feed_categories["roots"][0]+feed_categories["sugarcrops"][0]+feed_categories["sugar"][0]+feed_categories["pulses"][0]+feed_categories["nuts"][0]+feed_categories["oilcrops"][0]+feed_categories["oil"][0]+feed_categories["fruitnveg"][0]
feed_items_in_production = feed_categories["cereal"][1]+feed_categories["roots"][1]+feed_categories["sugarcrops"][1]+feed_categories["sugar"][1]+feed_categories["pulses"][1]+feed_categories["nuts"][1]+feed_categories["oilcrops"][1]+feed_categories["oil"][1]+feed_categories["fruitnveg"][1]
feed_items_conversions = feed_categories["cereal"][2]+feed_categories["roots"][2]+feed_categories["sugarcrops"][2]+feed_categories["sugar"][2]+feed_categories["pulses"][2]+feed_categories["nuts"][2]+feed_categories["oilcrops"][2]+feed_categories["oil"][2]+feed_categories["fruitnveg"][2]
feed_balance_production_mappings = {v[0]:v[1] for v in zip(feed_items_in_balance,feed_items_in_production)}
feed_production_balance_mappings = {v[1]:v[0] for v in zip(feed_items_in_balance,feed_items_in_production)}

bovine_meat_codes = [867,947,1097,1108,1124,1127]#[867,947,977,1017,1097,1108,1124,1127]
bovine_codes = [866,946,1096,1107,1110,1126]
ovine_meat_codes = [977,1017]
ovine_codes = [976,1016]
milk_codes = [882,951,982,1020,1130]
pig_meat_codes = [1035]
pig_codes = [1034]
poultry_meat_codes = [1058,1069,1073,1080,1089]
poultry_codes = [1057,1068,1072,1079,1083]
egg_codes = [1062,1091]
meat_animal_mappings = {867:866,947:946,1097:1096,1108:1107,1124:1110,1127:1126,977:976,1017:1016,1035:1034,1058:1057,1069:1068,1073:1072,1080:1079,1089:1083}
meat_codes = meat_animal_mappings.keys()
animal_meat_mappings = {v:k for k,v in meat_animal_mappings.iteritems()}
milkeggs_meat_mappings = {882:867,951:947,982:977,1020:1017,1062:1058,1130:1127,1091:1069}
meat_milkeggs_mappings = {867:882,947:951,977:982,1017:1020,1058:1062,1127:1130,1069:1091}
milkeggs_animal_mappings = {882:866,951:946,982:976,1020:1016,1062:1057,1130:1126,1091:1068}
milkeggsmeat_animal_mappings = dict(milkeggs_animal_mappings.items()+meat_animal_mappings.items())
animal_milkeggs_mappings = {v:k for k,v in milkeggs_animal_mappings.iteritems()}
producing_animals_group = 31
producing_animals_codes = [5320,5322,5318,5321,5313,5323,5319,5314]
khead_codes = [5321,5313,5323]
milking_codes = [5318]
laying_codes = [5313]
processed_codes = [5130]
items_that_use_pasture = bovine_meat_codes+ovine_meat_codes+milk_codes

#the following is for items in cropproduction that aren't in tradecropslivestock
trade_to_production_mappings = {27:(38,0.637),254:(258,0.0276),277:(278,0.1),305:(306,0.15),310:(311,0.66),328:(331,0.1),542:(515,1.0),674:(677,1.0)}
fodder_to_crop_mappings = {637:83,638:71,644:358,645:394,648:426}


#feedcode_mappings_production = {0:[1717], 1:[638,639,640,641,642,643], 2:[1720,1726,1732], 3:[1735], 4:[1726,1732]}
#feedcode_mappings_balance = {0:[2905],1:[638,639,640,641,642,643], 2:[2907,2913,2911], 3:[2918], 4:[2913,2911]}
cmpndfeed_mappings = {840:867,841:1058,842:1035,845:1058}
#landless_animal_codes = [1034,1057,1183,1089,1069,1163,1073,1062,1067,1182,1084,1094,1070,1077,1055,1144,1154,1087,1151,1167,1091,1092,1083,1141,1185,1195,1176,999,1080]

#get the countrymappings
region_codes = {5000:"World",5101:"Eastern Africa",5102:"Middle Africa",5103:"Northern Africa",5104:"Southern Africa",5105:"Western Africa",5203:"Northern America",5204:"Central America",5206:"Carribbean",5207:"South America",5301:"Central Asia",5302:"Eastern Asia",5303:"Southern Asia",5304:"South-Eastern Asia",5305:"Western Asia",5401:"Eastern Europe",5402:"Northern Europe",5403:"Southern Europe",5404:"Western Europe",5501:"Australia and New Zealand",5502:"Melanesia",5503:"Micronesia",5504:"Polynesia",5100:"Africa",5200:"Americas",5300:"Asia",5400:"Europe",5500:"Oceania",5706:"European Union"}#,5600:"Antarctic Region"
continent_codes = {5100:"Africa",5200:"Americas",5300:"Asia",5400:"Europe",5500:"Oceania",}
world_code = 5000
china_producing_code = 351
china_trade_code = 357
country_mappings = get_country_mappings()
crop_codes = get_crop_codes()
livestockprimary_codes = get_livestockprimary_codes()
livestock_codes = get_livestock_codes()

primary2commodity_mappings = {
	515:2617,
	486:2615,
	44:2513,
	176:2546,
	125:2532,
	89:2520,
	92:2520,
	94:2520,
	97:2520,
	101:2520,
	103:2520,
	108:2520,
	512:2614,
	698:2642,
	661:2633,
	252:2578,
	249:2560,
	656:2630,
	331:2575,
	577:2619,
	521:2625,
	523:2625,
	526:2625,
	530:2625,
	531:2625,
	534:2625,
	536:2625,
	541:2625,
	542:2625,
	544:2625,
	547:2625,
	549:2625,
	550:2625,
	552:2625,
	554:2625,
	558:2625,
	567:2625,
	568:2625,
	569:2625,
	571:2625,
	572:2625,
	587:2625,
	591:2625,
	592:2625,
	600:2625,
	603:2625,
	619:2625,
	507:2613,
	560:2620,
	244:2572,
	242:2556,
	497:2612,
	56:2514,
	60:2582,
	79:2517,
	216:2551,
	217:2551,
	220:2551,
	221:2551,
	222:2551,
	223:2551,
	224:2551,
	225:2551,
	226:2551,
	75:2516,
	#excluded 2586
	263:2570,
	265:2570,
	275:2570,
	277:2570,
	280:2570,
	296:2570,
	299:2570,
	305:2570,
	310:2570,
	311:2570,
	312:2570,
	333:2570,
	336:2570,
	339:2570,
	261:2580,
	260:2563,
	403:2602,
	490:2611,
	257:2577,
	258:2576,
	187:2547,
	687:2640,
	689:2641,
	574:2618,
	489:2616,
	116:2531,
	181:2549,
	191:2549,
	195:2549,
	197:2549,
	201:2549,
	203:2549,
	205:2549,
	210:2549,
	211:2549,
	271:2574,
	293:2574,
	27:2805,
	36:2581,
	135:2534,
	136:2534,
	149:2534,
	71:2515,
	289:2561,
	290:2579,
	83:2518,
	237:2571,
	236:2555,
	692:2645,
	693:2645,
	702:2645,
	711:2645,
	720:2645,
	723:2645,
	158:2542,
	159:2542,
	157:2537,
	156:2536,
	163:2541,
	267:2557,
	268:2573,
	122:2533,
	#Sweeteners, Other excluded
	667:2635,
	388:2601,
	358:2605,
	366:2605,
	367:2605,
	372:2605,
	373:2605,
	378:2605,
	393:2605,
	394:2605,
	397:2605,
	399:2605,
	401:2605,
	402:2605,
	406:2605,
	407:2605,
	414:2605,
	417:2605,
	420:2605,
	423:2605,
	426:2605,
	430:2605,
	447:2605,
	449:2605,
	459:2605,
	461:2605,
	463:2605,
	567:2605,
	568:2605,
	15:2511,
	564:2644,
	137:2535,
	867:2731,
	#Butter, Ghee excluded
	1062:2744,
	1182:2745,
	1089:2735,
	1097:2735,
	1108:2735,
	1124:2735,
	1111:2735,
	1127:2735,
	1141:2735,
	1151:2735,
	1158:2735,
	1163:2735,
	882:2848,
	951:2848,
	1020:2848,
	1089:2848,
	1130:2848,
	982:2848,
	977:2732,
	1035:2733,
	1058:2734,
	1069:2734,
	1073:2734,
	1080:2734,
}

			
			
		

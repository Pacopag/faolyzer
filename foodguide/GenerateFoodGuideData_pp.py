import sys, time
sys.path.append('../faotools/')
import FAOTools
import foodguide
import codecs
import pp

ppservers = ()
job_server = None

def main():
	startTime = time.time()
	
	if len(sys.argv) > 1:
		ncpus = int(sys.argv[1])
		# Creates jobserver with ncpus workers
		job_server = pp.Server(ncpus, ppservers=ppservers)
	else:
		# Creates jobserver with automatically detected number of workers
		job_server = pp.Server(ppservers=ppservers)
	job_server = pp.Server(8, ppservers=ppservers)
	ncpus = job_server.get_ncpus()
	print "Starting pp with", ncpus, "workers"
	
	years = range(1961,2010)
	trade_partners = dict(FAOTools.get_countries(FAOTools.table_balancers,'dict').items() + FAOTools.region_codes.items())
	del trade_partners[277] #South Sudan and Sudan isn't there.
	del trade_partners[276]
	#trade_partners = dict(FAOTools.continent_codes.items())
	#trade_partners = {231:"United States of America"}
	#livestockprimary_codes = FAOTools.bovine_meat_codes + FAOTools.ovine_meat_codes + FAOTools.pig_meat_codes + FAOTools.poultry_meat_codes + FAOTools.milk_codes + FAOTools.egg_codes
	
	#livestockprimary_codes = FAOTools.poultry_meat_codes
	#livestockprimary_codes = [867]
	#[867,882,947,951,977,982,987,1017,1020,1035,1058,1062,1069,1073,1080,1089,1091,1097,1108,1124,1127,1130,1141,1158]
	#crops = {rec['itemcode']:rec['item'] for rec in FAOTools.db.cropsproduced.find({'itemcode':{'$in':FAOTools.crop_codes}})}
	
	tot_num = len(trade_partners)
	num_done = 0
	
	jobs = []
	country_item = []
	for country_code,country in trade_partners.iteritems():
		temp_code = FAOTools.country_mappings[country_code] if country_code in FAOTools.country_mappings else country_code
		jobs.append(job_server.submit(doIt, args=(years,country_code,country,temp_code), modules=("FAOTools","foodguide",)))
		country_item.append(country)
		
	with codecs.open('foodguidelandspared.csv', 'w', 'utf-8-sig') as f:
		header = '"CountryCode","Country","ItemCode","Item","ElementGroup","ElementCode","Element","Year","Unit","Value","Flag"\r\n'
		f.write(header)
		for job,ci in zip(jobs,country_item):
			f.write(unicode(job()))
			num_done += 1
			try:
				print str(1.0*num_done/tot_num*100)+'%',ci
			except UnicodeEncodeError:
				print str(1.0*num_done/tot_num*100),"Unicode error (don't worry)"
	
	job_server.wait()    
	job_server.print_stats()
	job_server.destroy()
	
	print "Total time elapsed: ", time.time() - startTime, "s"	
			
def doIt(years,country_code,country,temp_code):
	lines = ""
	for year in years:
		ls = foodguide.get_land_saved_by_food_guide(year,country_code)
		flg = ""
		
		lsot = ls['total']['total']	
		lsol = ls['total']['local']
		lsor = ls['total']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","0","All","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","0","All","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","0","All","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
		lsot = ls['grains']['total']	
		lsol = ls['grains']['local']
		lsor = ls['grains']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","1","Grains","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","1","Grains","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","1","Grains","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
		lsot = ls['fruits']['total']	
		lsol = ls['fruits']['local']
		lsor = ls['fruits']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","2","Fruits","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","2","Fruits","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","2","Fruits","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
		lsot = ls['vegetables']['total']	
		lsol = ls['vegetables']['local']
		lsor = ls['vegetables']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","3","Vegetables","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","3","Vegetables","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","3","Vegetables","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
		lsot = ls['oils']['total']	
		lsol = ls['oils']['local']
		lsor = ls['oils']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","4","Oils","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","4","Oils","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","4","Oils","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
		lsot = ls['meats']['total']	
		lsol = ls['meats']['local']
		lsor = ls['meats']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","5","Meats","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","5","Meats","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","5","Meats","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
		lsot = ls['dairy']['total']	
		lsol = ls['dairy']['local']
		lsor = ls['dairy']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","6","Dairy","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","6","Dairy","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","6","Dairy","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
		lsot = ls['discretional']['total']	
		lsol = ls['discretional']['local']
		lsor = ls['discretional']['remote']
		
		lines += u'"'+str(country_code)+'","'+country+'","7","Discretional","-41","-100","Total","'+str(year)+'","Ha","'+str(lsot)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","7","Discretional","-41","-101","Domestic","'+str(year)+'","Ha","'+str(lsol)+'","'+flg+'"\r\n'
		lines += u'"'+str(country_code)+'","'+country+'","7","Discretional","-41","-102","Displaced","'+str(year)+'","Ha","'+str(lsor)+'","'+flg+'"\r\n'
		
	return lines
	
    
if __name__=="__main__":
    #dbtype = sys.argv[1]
    main()

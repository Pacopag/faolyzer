import sys
sys.path.append('../faotools/')
from FAOTools import *
from calorie_intakes import *
from code_defns import *

foods_balanced = b2p_mappings.keys()
fbs_codes = [food_code_fb,food_supply_code_fb,domestic_supply_code_fb,import_code_fb] #parts of food balance sheet that interest us.

results_groups = {'fruits':[],'vegetables':[],'grains':[],'meats':[],'dairy':[],'oils':[],'sugar':[]}
results_groups_imports = {'fruits':[],'vegetables':[],'grains':[],'meats':[],'dairy':[],'oils':[],'sugar':[]}
results_groups_domestic = {'fruits':[],'vegetables':[],'grains':[],'meats':[],'dairy':[],'oils':[],'sugar':[]}
results_total = []
results_total_imports = []
results_total_domestic = []

def get_land_saved_by_food_guide(year,country_code,calorie_level=2000):

	start_year = 1961
	end_year = 2009
	if country_code < world_code:
		spec = {'countrycode':country_code}
		fields = {'start_year':1,'end_year':1}
		rec,f = find_one(table_balancers,spec,fields)
		if rec is None:
			print "Country not found",country_code
			raise ValueError
		start_year = 1961 if rec['start_year']=='' else rec['start_year']
		end_year = 2009 if rec['end_year']=='' else rec['end_year']
	
	if year<start_year:
		return get_land_saved_by_food_guide(start_year,country_code)
	elif year>end_year:
		return get_land_saved_by_food_guide(end_year,country_code)

	tot_land_saved = 0.0
	tot_land_saved_imports = 0.0
	tot_land_saved_domestic = 0.0
	
	#Pre-fetch world-average yields to use as estimated land use due to imports.
	world_yields = {}
	for fb in foods_balanced:
		pcs = b2p_mappings[fb] if fb!=butter_code else b2p_mappings[milk_code]
		yld,f = get_weighted_yield(year,world_code,pcs)
		world_yields[fb] = yld
		#print "World yield ",yld

	spec = {'year':year, 'countrycode':country_code, 'itemcode':population_item_code, 'elementcode':population_element_code}
	fields = {'value':1}
	rec,f = find_one(table_population, spec, fields, [],'year', None)
	pop = 1000.0*rec['value'] #Population is reported in units of 1000 people, hence the conversion.

	#print "Population\t".pop."\n"
	for fg in food_groups:
		res_land_saved = 0.0
		res_land_saved_imports = 0.0
		res_land_saved_domestic = 0.0
		
		# Get country's "weighting" trend for the group components (e.g. %wine and %fruit for "fruits" group)
		group_components = food_groups[fg]
		component_weights = {}
		component_total = 0.0
		spec = {'year':year, 'countrycode':country_code, 'itemcode':{'$in':group_components}, 'elementcode':food_supply_code_fb}
		fields = {'itemcode':1,'value':1}
		qry,f = find(table_foodbalance,spec,fields)
		for r in qry:
			v = r['value'] if bool(r['value']) else 0.0
			component_weights[r['itemcode']] = v
			component_total += v
		
		for fb in group_components: #(foods_balanced as fb) {
			#print "-------------------------------------------\n"
			component_weights[fb] = component_weights[fb]/component_total if fb in component_weights and component_total!=0 else 0.0  #Normalizes the component weights.
			#print "Component weight ".component_weights[fb]."\n"			
			spec = {'year':year, 'countrycode':country_code, 'itemcode':fb, 'elementcode':{'$in':fbs_codes}}
			fields = {'elementcode':1,'value':1}
			qry,f = find(table_foodbalance, spec, fields)
			(food,domestic,imports,supply) = (0.0,0.0,0.0,0.0)
			for r in qry:
				v = float(r['value']) if r['value']!='' else 0.0
				if r['elementcode']==food_code_fb:
				#Note: food balance sheets report quantities in kilotonnes
				#      so we multiply by 1000.0 to convert to tonnes.
					food = 1000.0*v
				elif r['elementcode']==domestic_supply_code_fb:
					domestic = 1000.0*v
				elif r['elementcode']==import_code_fb:
					imports = 1000.0*v
				elif r['elementcode']==food_supply_code_fb:
					supply = v
			
			#print "Food\t\t".food."\n"
			#print "Supply\t\t".supply."\n"
			#print "Domestic\t".domestic."\n"
			#print "Import\t\t".import."\n"

			idr = imports/domestic if domestic!=0 else 1.0
			import_adj = (idr*food)/b2p_conversions[fb] #import_adj now means imported "food".
			food_adj = (food - import_adj)/b2p_conversions[fb] #food_adj now means locally produced "food".

			#print "IDR\t".idr."\n"
			#print "Food (adj)\t".food_adj."\n"
			#print "Import (adj)\t".import_adj."\n"
						
			pcs = b2p_mappings[fb] if fb!=butter_code else b2p_mappings[milk_code]
			yld,f = get_weighted_yield(year,country_code,pcs)
			if not bool(yld):
				idr = 1.0 # Production comes from processed imports (e.g. Canada and Sugars)
				import_adj = import_adj + food_adj
				food_adj = 0.0
				yld = 1.0#just a dummy value
			#print "Yield ".country_code.":".implode(",",pcs).":".yield."\n"
			land_local = food_adj/yld
			#print "Local land use ".land_local."\n"
			if world_yields[fb]==0:
				print year,fb
			land_remote = import_adj/world_yields[fb]
			#print "Remote land use ".land_remote."\n"
			land_total = land_local+land_remote
			#print "Total land use ".land_total."\n"
			rec_cal = component_weights[fb]*calorie_intakes[calorie_level][fg]
			#print "Recommended daily calories ".rec_cal."\n"
			#print "Population ".pop."\n"
			tot_rec_cal = rec_cal*pop*365
			#print "Recommended annual calories ".tot_rec_cal."\n"
			kg_per_cal = food/(supply*pop*365) if tot_rec_cal!=0 else 0.0
			#print "kg per cal ".kg_per_cal."\n"
			tot_rec_kg = tot_rec_cal*kg_per_cal
			#print "Recommended kg food ".tot_rec_kg."\n"
			rec_import = idr*tot_rec_kg
			#print "Recommended import ".rec_import."\n"
			rec_food = tot_rec_kg - rec_import
			#print "Recommended food (local)".rec_food."\n"	
			rec_land_local = rec_food/yld if bool(yld) else 0.0
			#print "Recommended local land use ".rec_land_local."\n"
			rec_land_remote = rec_import/world_yields[fb]
			#print "Recommended remote land use ".rec_land_remote."\n"
			rec_land_total = rec_land_local+rec_land_remote
			#print "Recommended total land use ".rec_land_total."\n"
			diff_land_local = land_local-rec_land_local
			#print "Local land saved ".diff_land_local."\n"
			diff_land_remote = land_remote-rec_land_remote
			#print "Remote land saved ".diff_land_remote."\n"
			diff_land_total = land_total-rec_land_total
			#print "Total land saved ".diff_land_total."\n"
			
			res_land_saved += diff_land_total
			res_land_saved_imports += diff_land_remote
			res_land_saved_domestic += diff_land_local
			tot_land_saved += diff_land_total
			tot_land_saved_imports += diff_land_remote
			tot_land_saved_domestic += diff_land_local
			
		results_groups[fg] = res_land_saved
		results_groups_imports[fg] = res_land_saved_imports
		results_groups_domestic[fg] = res_land_saved_domestic
	# end foreach food_groups
	results_total = tot_land_saved
	results_total_imports = tot_land_saved_imports
	results_total_domestic = tot_land_saved_domestic
	
	return {
			'total':{'total':results_total,'local':results_total_domestic,'remote':results_total_imports},
			'fruits':{'total':results_groups['fruits'],'local':results_groups_domestic['fruits'],'remote':results_groups_imports['fruits']},
			'vegetables':{'total':results_groups['vegetables'],'local':results_groups_domestic['vegetables'],'remote':results_groups_imports['vegetables']},
			'grains':{'total':results_groups['grains'],'local':results_groups_domestic['grains'],'remote':results_groups_imports['grains']},
			'oils':{'total':results_groups['oils'],'local':results_groups_domestic['oils'],'remote':results_groups_imports['oils']},
			'discretional':{'total':results_groups['sugar'],'local':results_groups_domestic['sugar'],'remote':results_groups_imports['sugar']},
			'meats':{'total':results_groups['meats'],'local':results_groups_domestic['meats'],'remote':results_groups_imports['meats']},
			'dairy':{'total':results_groups['dairy'],'local':results_groups_domestic['dairy'],'remote':results_groups_imports['dairy']},
	}
# end for year in years



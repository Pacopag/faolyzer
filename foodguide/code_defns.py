cereal_codes_balance = [2905]
fruit_codes_balance = [2919]
oil_codes_balance = [2913]
meat_codes_balance = [2911]
vegetable_codes_balance = [2907,2918]
sugar_codes_balance = [2909]

butter_code = 2740
milk_code = 2948

food_groups = {

	'fruits'	:[2919,2655],
	'vegetables':[2918,2907],
	'grains'	:[2905,2656,2658],
	'meats'		:[2911,2912,2731,2732,2733,2734,2913,2949],
	'dairy'		:[2948, 2740],
	'oils'		:[2914],
	'sugar'		:[2908,2909,2922]
	
}


b2p_mappings = {
	2656:[44], 	#beer				*
	2658:[1717], 	#alcohol		*
	2655:[560], 	#wine			**
	2905:[1717], 	#cereal			*
	2919:[1801], 	#fruit				**
	2913:[1732], 	#oilcrops		***
	2911:[1726],	#pulses		*** ***
	2907:[1720],	#roots			****
	2909:[156,157,161],	#sugar			*****
	2908:[156,157,161],	#sugarcrops			*****
	2912:[1729],	#treenuts		*** ***
	2918:[1735],	#vegetables	****
	2922:[661,656],	#stimulants	*****
	2914:[1732],	#oils				***
	2731:[867],	#beef				*** ***
	2732:[977,1017],	#mutton	*** ***
	2733:[1035],	#pork			*** ***
	2734:[1058],	#poultry		*** ***
	2948:[882],		#milk			*** *** *
	2740:[886],		#butter		*** *** *
	2949:[1062],		#eggs		*** ***
	
	
}

b2p_conversions = {
	2656:4.78, 	#beer
	2658:0.6, 	#alcohol
	2655:0.7, 	#wine
	2905:1.0, 	#cereal
	2919:1.0, 	#fruit
	2913:1.0, 	#oilcrops
	2911:1.0,	#pulses
	2907:1.0,	#roots
	2909:0.12,	#sugar
	2908:1.0,	#sugarcrops
	2912:1.0,	#treenuts
	2918:1.0,	#vegetables
	2922:1.0,	#stimulants
	2914:0.2,	#oils
	2731:1.0,	#beef
	2732:1.0,	#mutton
	2733:1.0,	#pork
	2734:1.0,	#poultry
	2948:1.0,	#milk
	2740:0.047,	#butter
	2949:1.0,	#eggs
}

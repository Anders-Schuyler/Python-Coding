#enumerate example



#using enumerate with openpyxl to write dictionary values to cell
for count, value in enumerate(iteam_oreo.values(), 1):
    ws.cell(row=1, column=count, value=value)

#writting each dictionary to rows in the Excel sheet
for row_count, each_dict in enumerate(inventory_list):
    for col_count, value in 


#Example of dictionary of list
store = {
    "aisle_1": ["fruits", "vegetables", "dairy"],
    "aisle_2": ["meat", "frozen", "bread"],
    "aisle_3": ["poultry", "seafood", "dressing"],
    "aisle_4": ["cereal", "soup", "water"],
}

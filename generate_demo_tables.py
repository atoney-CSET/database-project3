#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 22 11:35:04 2023

@author: at1120
"""

def generate_table(table_name, n, col2_fixed):
    column1_name="y"
    column2_name="z"
    
    create_table = f"""CREATE TABLE {table_name} ({column1_name} INT, {column2_name} INT, PRIMARY KEY ({column1_name}));"""

    
    print(create_table.strip())
    
    
    insert_values = []
    
    for i in range(1,n+1):
        val1 = i 
        
        if col2_fixed:
            val2 = 1
        else:
            val2 = i
    
        insert_values.append(f"({val1}, {val2})")
        
    str_list = ','.join(insert_values)
    insert_row = f"""INSERT INTO {table_name} ({column1_name}, {column2_name}) VALUES {str_list};"""
    print(insert_row.strip())
        
        
for n in [1000, 10000, 100000]:
#for n in [10]:        
    generate_table(f"relii{n}", n, False)
    generate_table(f"reli1{n}", n, True)
            
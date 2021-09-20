# A python program for data cleansing 

## Flow Chart
![Alt Text](/DE/flow.png)

Example Usage:

```
python3 dataEnginnering.py
```

Requirement library:
```
pandas
numpy
os
re
datetime
```

## Workflow hightlight
1. In stage 1, I open the file in Excel and then check for the outlier columns in the file. 
Found that  ```Account_ID , Active Indicator ,Account Type ,Account status ,FIBRE ,Property TYPE ``` contains questionable data. 

2. In stage 2, I use pandas to read the dataFrame and write functions for the transformation for different columns. The function will return NULL for the field that is questionable.
Later on, I can use the conditional filtering on dataFrame to eliminate the outliers.
Lastly, I produce the ```Response_Time``` column for response ranking by post code.

3. In stage 3, I write a function 
```
def batch_write_json(df,out_dir='./'):
    import os
    length = len(df)
    number_of_file = 0
    last_file_number = 0
    last_carry = 0
    # divisible number of line
    if (length % 1000) == 0:
        number_of_file = length // 1000
        last_file_number = number_of_file
    else:
        number_of_file = length // 1000 + 1
        last_file_number = number_of_file
        last_carry = length % 1000
        
    print(f'number_of_file:{number_of_file},last_file_number:{last_file_number},last_carry:{last_carry}')
            
    # Starting file name
    current_index = 1
    # iterative through every file        
    while (current_index <= number_of_file):
        print(f"processing file: {current_index}")
        os.path.join(out_dir,f'{current_index}.json')
        with open(os.path.join(out_dir,f'{current_index}.json'), 'a') as outfile:
            if current_index == last_file_number:
                print("Last file!")
                if last_carry != 0:
                    json.dump(df[1000 * (current_index - 1) :1000 * (current_index - 1) + last_carry ].to_json(orient='records')[1:-1],outfile)
                elif last_carry == 0:
                    json.dump(df[1000 * (current_index - 1) : 1000 * current_index].to_json(orient='records')[1:-1],outfile)
            json.dump(df[1000 * (current_index - 1) : 1000 * current_index].to_json(orient='records')[1:-1],outfile)
        current_index = current_index + 1   
    
    print(f"Successfully wrote Json file to {outfile}")
    return True
``` 
to output the file in batches. I did not write the file line by line because I think that is less efficient. 


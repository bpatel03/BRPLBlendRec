import pandas as pd
from pulp import *
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import math
import json

sheet_id = "1nuNtz1jwXyd56AD5R-RQT2KRQXP-ConsV49HIZniSzo"
r = "https://docs.google.com/spreadsheets/d/{}/export?format=csv".format(sheet_id)
df = pd.read_csv(r)
df = df.dropna()
df['Stock'][df['Stock'] < 10] = 0

# Flatten the nested dictionary
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def append_row_to_google_sheet(credentials_file, sheet_title, new_row_data):
    # Authenticate using the service account credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
    gc = gspread.authorize(credentials)

    # Open the Google Sheet by its title
    sh = gc.open(sheet_title)

    # Select the first (and presumably only) worksheet
    worksheet = sh.get_worksheet(0)

    # Append a new row to the worksheet
    worksheet.append_row(new_row_data)


###########################################################
def BP_Act():
    now = datetime.now()
    #today = now.strftime("%d")
    sheet_id = "1eM3xk2P0OChfNDdOu7DWbUCb5C5mgchbMuxu6I5P2Q0"
    r = "https://docs.google.com/spreadsheets/d/{}/export?format=csv".format(sheet_id)
    df = pd.read_csv(r)
    df =df.fillna(0)
    df = df[df['Mine/Source_Supplier_PO Fe'] != 0]
    
    
    df_iof=df[['Mine/Source_Supplier_PO Fe', 'IOFPrice', 'Cl. stk', 'Fe', 'Si', 'Al',
           'LOI']]
    df_dt=df[[ '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
           '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24',
           '25', '26', '27', '28', '29', '30', '31']]
    # Replace 'NaN' values with 0
    df_dt = df_dt.fillna(0)
    Prod_dt = now + timedelta(days=-1)
    i=int(Prod_dt.strftime("%d"))
    df_dt=df_dt[:]
    df_dt=df_dt.iloc[:, i-1:i]
    date = Prod_dt.strftime("%m-%d-%y")
    
    # Master IOF details 
    column_name = df_dt.columns[0]
    df_dt[column_name] = pd.to_numeric(df_dt[column_name].str.replace(',', ''), errors='coerce')
    df_iof['Feed_MT'] = df_dt.iloc[:,0]
    
    df_iof = df_iof.fillna(0)
    df_iof = df_iof[df_iof['Fe'] != 0 ]


    #df_iof.loc[:, 'Feed_MT'] = df_dt.iloc[:, 0].copy()
    df_iof['Feed_MT'] = df_dt.iloc[:, 0].copy()
    df_iof = df_iof.fillna(0)
    df_day=df_iof.copy()
    df_day = df_day[df_day['Feed_MT'] != 0 ]
    df_day['IOF%'] = round(df_day['Feed_MT'] * 100 / df_day['Feed_MT'].sum(), 0)
    df_day['IOF_Tcost'] = round(df_day['Feed_MT'] * df_day['IOFPrice'], 0)
    
    TotCost=df_day['IOF_Tcost'].sum()
    TotFeed=df_day['Feed_MT'].sum()
    CostPerMT=round(TotCost/TotFeed,2)
    
    # Calculate the weighted average using the sumproduct method
    wt_fe = round((df_day['Fe'] * df_day['Feed_MT']).sum() / df_day['Feed_MT'].sum(),2)
    wt_si = round((df_day['Si'] * df_day['Feed_MT']).sum() / df_day['Feed_MT'].sum(),2)
    wt_al = round((df_day['Al'] * df_day['Feed_MT']).sum() / df_day['Feed_MT'].sum(),2)
    wt_loi = round((df_day['LOI'] * df_day['Feed_MT']).sum() / df_day['Feed_MT'].sum(),2)
    
    print("Actual Price/mt :",CostPerMT," W_Fe :",wt_fe," W_Si :",wt_si," W_Al :",wt_al," W_loi :",wt_loi,"\n\n")
    # Convert DataFrame to dictionary with 'Name' column as keys
    result_dict = df_day.set_index('Mine/Source_Supplier_PO Fe').to_dict(orient='index')
    print(result_dict,"\n\n")
    return TotFeed,CostPerMT,wt_fe,wt_si,wt_al,wt_loi,result_dict,date
###########################################################

    
def login_open_sheet(oauth_key_file, spreadsheet):
    """Connect to Google Docs spreadsheet and return the first worksheet."""
    try:
        scope =  ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(oauth_key_file, scope)
        gc = gspread.authorize(credentials)
        worksheet = gc.open(spreadsheet).sheet1
        return worksheet
    except Exception as ex:
        print('Unable to login and get spreadsheet.  Check OAuth credentials, spreadsheet name, and make sure spreadsheet is shared to the client_email address in the OAuth .json file!')
        print('Google sheet login failed with error:', ex)
        sys.exit(1)

def blend_Opt_nostock(df,fe,si,al,loi):
    
    df1 = df
    

    
    # Convert DataFrame columns to separate dictionaries
    ores = df1['Mines'].tolist()
    prices = df1.set_index('Mines')['Prices'].to_dict()
    Initial_Stocks = df1.set_index('Mines')['Stock'].to_dict()
    fe_percentages = df1.set_index('Mines')['Fe%'].to_dict()
    al_percentages = df1.set_index('Mines')['AL%'].to_dict()
    si_percentages = df1.set_index('Mines')['SI%'].to_dict()
    loi_percentages = df1.set_index('Mines')['LOI%'].to_dict()
    
    res_rows=[]
    #********************#
    FE = fe
    SI = si
    AL = al
    LOI = loi

    for upBound in range(0, 11):
        upBound /= 10  # Convert to decimal
        
        # Create the linear programming problem
        prob = LpProblem("Ore_Blending", LpMinimize)
            
        # Define the variables
        ore_vars = LpVariable.dicts("Ore", ores, lowBound=0, upBound=upBound, cat='Continuous')
    
        # Add a constraint to ensure ore_vars[ore] is non-negative for each ore
        for ore in ores:
            prob += ore_vars[ore] >= 0
                
        # Define the objective function (minimize the total price)
        prob += lpSum([prices[ore] * ore_vars[ore] for ore in ores])
        # Define the constraints
        prob += lpSum([ore_vars[ore] for ore in ores]) == 1.0  # Constraint: Total percentage is 100%
        prob += lpSum([fe_percentages[ore] * ore_vars[ore] for ore in ores]) >= FE  # Constraint: Fe% is at least 63
        prob += lpSum([fe_percentages[ore] * ore_vars[ore] for ore in ores]) <= FE+0.3
        prob += lpSum([al_percentages[ore] * ore_vars[ore] for ore in ores]) <= AL  # Constraint: Al% is less than or equal to 3
        prob += lpSum([al_percentages[ore] * ore_vars[ore] for ore in ores]) >= AL-0.3
        prob += lpSum([loi_percentages[ore] * ore_vars[ore] for ore in ores]) <= LOI
            
        # Solve the problem
        prob.solve()
            
        # Calculate the blended Fe%, AL%, SI%, and LOI%
        blended_fe = sum([value(ore_vars[ore]) * fe_percentages[ore] for ore in ores])
        blended_al = sum([value(ore_vars[ore]) * al_percentages[ore] for ore in ores])
        blended_si = sum([value(ore_vars[ore]) * si_percentages[ore] for ore in ores])
        blended_loi = sum([value(ore_vars[ore]) * loi_percentages[ore] for ore in ores])
        day=1
        # Check the status of the solution
        if LpStatus[prob.status] != "Optimal":
            print(f"For day={day}, no optimal solution found.")
        else:
                
            #break
            res_rows.append( [ round(value(prob.objective))]+[round(value(ore_vars[ore]) * 100, 2) for ore in ores] )

        

    # Create the DataFrame after the loop
    res_df = pd.DataFrame(res_rows, columns= ['BlendCost']+[''+ ore +'' for ore in ores] )
    res_df = res_df.sort_values(by='BlendCost')
    res_df = res_df.iloc[:1, :].copy()
    res_df = res_df.transpose()
    res_df.columns = ['Feed%']
    res_df = res_df[res_df['Feed%'] != 0]
    res_df = res_df.iloc[1:, :].copy()
    
    #***************AI Prediction*******************#
    AI_Pred_Cost= round(value(prob.objective))
    AI_Pred_FE =  round(blended_fe, 2)
    AI_Pred_SI =  round(blended_si, 2)
    AI_Pred_AL =  round(blended_al, 2)
    AI_Pred_LOI=  round(blended_loi, 2)
    Pred_IOF_dict = res_df.to_dict(orient='index')
    print("AI_Predicted :-","\n Cost :",AI_Pred_Cost,"\n Fe :",AI_Pred_FE,"\n Si :",AI_Pred_SI,"\n Al :",AI_Pred_AL,"\n loi :",AI_Pred_LOI)
    print( "IOF_Source :",Pred_IOF_dict)
    

    return AI_Pred_Cost,AI_Pred_FE,AI_Pred_SI,AI_Pred_AL,AI_Pred_LOI,Pred_IOF_dict
    #return res_df

def blend_Opt_withstock(df,act_tot_feed,fe,si,al,loi):
    
    df1 = df
   

    
    # Convert DataFrame columns to separate dictionaries
    ores = df1['Mines'].tolist()
    prices = df1.set_index('Mines')['Prices'].to_dict()
    Initial_Stocks = df1.set_index('Mines')['Stock'].to_dict()
    fe_percentages = df1.set_index('Mines')['Fe%'].to_dict()
    al_percentages = df1.set_index('Mines')['AL%'].to_dict()
    si_percentages = df1.set_index('Mines')['SI%'].to_dict()
    loi_percentages = df1.set_index('Mines')['LOI%'].to_dict()
    
    res_rows=[]
    #********************#
    FE = fe
    SI = si
    AL = al
    LOI = loi

    for upBound in range(0, 11):
        upBound /= 10  # Convert to decimal
        
        # Create the linear programming problem
        prob = LpProblem("Ore_Blending", LpMinimize)
            
        # Define the variables
        ore_vars = LpVariable.dicts("Ore", ores, lowBound=0, upBound=upBound, cat='Continuous')
    
        # Add a constraint to ensure ore_vars[ore] is non-negative for each ore
        for ore in ores:
            prob += ore_vars[ore] >= 0
            prob += ore_vars[ore] <= Initial_Stocks[ore]
    
            
        # Define the objective function (minimize the total price)
        prob += lpSum([prices[ore] * ore_vars[ore] for ore in ores])
        # Define the constraints
        prob += lpSum([ore_vars[ore] for ore in ores]) == 1.0  # Constraint: Total percentage is 100%
        prob += lpSum([fe_percentages[ore] * ore_vars[ore] for ore in ores]) >= FE  # Constraint: Fe% is at least 63
        prob += lpSum([fe_percentages[ore] * ore_vars[ore] for ore in ores]) <= FE+0.3
        prob += lpSum([al_percentages[ore] * ore_vars[ore] for ore in ores]) <= AL  # Constraint: Al% is less than or equal to 3
        prob += lpSum([al_percentages[ore] * ore_vars[ore] for ore in ores]) >= AL-0.3
        prob += lpSum([loi_percentages[ore] * ore_vars[ore] for ore in ores]) <= LOI+.4
        prob += lpSum([loi_percentages[ore] * ore_vars[ore] for ore in ores]) >= LOI-.1

        # Add additional constraints for optimizing consumption ratio
        prob += lpSum([ore_vars[ore] for ore in ores]) == 1  # Constraint: Total percentage is 100%
        for ore in ores:
            prob += ore_vars[ore] <= Initial_Stocks[ore] / act_tot_feed # Constraint: Available stocks

            
        # Solve the problem
        prob.solve()
            
        # Calculate the blended Fe%, AL%, SI%, and LOI%
        blended_fe = sum([value(ore_vars[ore]) * fe_percentages[ore] for ore in ores])
        blended_al = sum([value(ore_vars[ore]) * al_percentages[ore] for ore in ores])
        blended_si = sum([value(ore_vars[ore]) * si_percentages[ore] for ore in ores])
        blended_loi = sum([value(ore_vars[ore]) * loi_percentages[ore] for ore in ores])
        day=1
        # Check the status of the solution
        if LpStatus[prob.status] != "Optimal":
            print(f"For day={day}, no optimal solution found.")
        else:
                
            #break
            res_rows.append( [ round(value(prob.objective))]+[round(value(ore_vars[ore]) * 100, 2) for ore in ores] )
    
    # Create the DataFrame after the loop
    res_df = pd.DataFrame(res_rows, columns= ['BlendCost']+[''+ ore +'' for ore in ores] )
    res_df = res_df.sort_values(by='BlendCost')
    res_df = res_df.iloc[:1, :].copy()
    res_df = res_df.transpose()
    res_df.columns = ['FeeD%']
    res_df = res_df[res_df['FeeD%'] != 0]
    res_df = res_df.iloc[1:, :].copy()
    
    #***************AI Prediction*******************#
    AI_Pred_Cost= round(value(prob.objective))
    AI_Pred_FE =  round(blended_fe, 2)
    AI_Pred_SI =  round(blended_si, 2)
    AI_Pred_AL =  round(blended_al, 2)
    AI_Pred_LOI=  round(blended_loi, 2)
    Pred_IOF_dict = res_df.to_dict(orient='index')
    print("AI_Predicted :-","\n Cost :",AI_Pred_Cost,"\n Fe :",AI_Pred_FE,"\n Si :",AI_Pred_SI,"\n Al :",AI_Pred_AL,"\n loi :",AI_Pred_LOI)
    print( "IOF_Source :",Pred_IOF_dict)
    

    return AI_Pred_Cost,AI_Pred_FE,AI_Pred_SI,AI_Pred_AL,AI_Pred_LOI,Pred_IOF_dict
    #return res_df    

while True:
    error_occurred = False
    
    try:
        
        bp_act=BP_Act()
        act_tot_feed=bp_act[0]
        act_Cost= bp_act[1]
        act_fe = bp_act[2]
        act_si = bp_act[3]
        act_al = bp_act[4]
        act_loi = bp_act[5]
        act_dict = bp_act[6]
        prod_dt = bp_act[7]
        act_dict = json.dumps(act_dict, indent=2)
        print("bp_Act",act_tot_feed)
        
        bl_opt_withStock= blend_Opt_withstock(df,act_tot_feed,act_fe,act_si,act_al,act_loi)
        
        WS_IOF_cost = bl_opt_withStock[0]
        WS_fe = bl_opt_withStock[1]
        WS_si = bl_opt_withStock[2]
        WS_al = bl_opt_withStock[3]
        WS_loi = bl_opt_withStock[4]
        WS_IOF_dict = bl_opt_withStock[5]
        WS_IOF_dict = json.dumps(WS_IOF_dict, indent=2)
        pl_WS = act_Cost-WS_IOF_cost
        
        
        bl_opt_noStock= blend_Opt_nostock(df,act_fe,act_si,act_al,act_loi)
        
        NS_IOF_cost = bl_opt_noStock[0]
        NS_fe = bl_opt_noStock[1]
        NS_si = bl_opt_noStock[2]
        NS_al = bl_opt_noStock[3]
        NS_loi = bl_opt_noStock[4]
        NS_IOF_dict = bl_opt_noStock[5]
        NS_IOF_dict = json.dumps(NS_IOF_dict, indent=2)
        pl_NS = act_Cost-NS_IOF_cost

        credentials_file = 'iofblend-4732b487dc0c.json'
        sheet_title = "BlendRecomendation"
    
        # New row data to append
        new_row_data = [prod_dt,act_tot_feed,act_Cost,act_fe,act_si,act_al,act_loi,act_dict,WS_IOF_cost,WS_fe,WS_si,WS_al,WS_loi,pl_WS,WS_IOF_dict,NS_IOF_cost,NS_fe,NS_si,NS_al,NS_loi,pl_NS,NS_IOF_dict]
        
        # Call the function to append the new row
        append_row_to_google_sheet(credentials_file, sheet_title, new_row_data)
                
        
    except ZeroDivisionError:
        print("Error: Cannot divide by zero. Please enter a non-zero number.")
        error_occurred = True
        
    except ValueError:
        print("Error: Please enter a valid integer.")
        error_occurred = True
    
    except Exception as e:
        print("An unexpected error occurred:", e)
        # Exit the loop if an unexpected error occurs
    
    else:
        # Code to run if no exception occurred
        print("No error occurred.")
        break  # Exit the loop if no error occurred
    
    finally:
        # Code to run whether an exception occurred or not
        print("This is the 'finally' block.")




    
    


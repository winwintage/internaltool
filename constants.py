query = "INSERT INTO {} ({}) VALUES ({});"

order_data_table = 'Order_Data'
sku_data_table = 'Sku_Data'
email_validation_table = 'Email_Validation'
order_status_table = 'Order_Status'

channel_list_table = 'Channel_List'
fabric_code_table = 'Fabric_Codes'
product_type_table = 'Product_Types'

sms_failed_table = 'SMS_Failed'
sms_opened_table = 'SMS_Opened'
sms_sent_table = 'SMS_Sent'

whatsapp_failed_table = 'Whatsapp_Failed'
whatsapp_opened_table = 'Whatsapp_Opened'
whatsapp_sent_table = 'Whatsapp_Sent'
whatsapp = 'Whatsapp'

zero_order_customers = 'Zero_Order_Customers'

table_list = ['Order_Data', 'Sku_Data', 'Email_Validation', 'Order_Status',
              'Channel_List', 'Fabric_Codes', 'Product_Types', 'SMS_Failed', 'SMS_Opened',
              'SMS_Sent', 'Whatsapp_Failed', 'Whatsapp_Opened', 'Whatsapp_Sent', 'Whatsapp', 'Zero_Order_Customers']


def sanitizeData(value):
    if value in ('', None):
        return "NULL"
    if type(value) is str:
        return "'{}'".format(value.replace("'", "''").strip())
    return value


def getQuery(table_name, values):
    if table_name == order_data_table:
        return """
                INSERT INTO %s
                (Sale_Order_Item_Code, Display_Order_Code, 
                Notifcation_Email, Notification_Mobile, 
                Shipping_Address_Name, Shipping_Address_Line1, 
                Shipping_Address_Line2, Shipping_Address_City, 
                Shipping_Address_State, Shipping_Address_Country, 
                Shipping_Address_Pincode, Order_Date, 
                Channel_Id, Item_SKU_Id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,(SELECT id from Channel_List WHERE Channel_Name = %s),(SELECT id from Sku_Data WHERE Item_Sku_Code = %s));
        """ % (
            table_name,
            sanitizeData(values['Sale_Order_Item_Code']),
            sanitizeData(values['Display_Order_Code']),
            sanitizeData(values['Notification_Email']),
            sanitizeData(values['Notification_Mobile']),
            sanitizeData(values['Shipping_Address_Name']),
            sanitizeData(values['Shipping_Address_Line1']),
            sanitizeData(values['Shipping_Address_Line2']),
            sanitizeData(values['Shipping_Address_City']),
            sanitizeData(values['Shipping_Address_State']),
            sanitizeData(values['Shipping_Address_Country']),
            sanitizeData(values['Shipping_Address_Pincode']),
            sanitizeData(values['Order_Date']),
            sanitizeData(values['Channel_Name']),
            sanitizeData(values['Item_SKU_Code'])
        )
    elif table_name == sku_data_table:
        return """
                INSERT INTO %s 
                (Item_Sku_Code, Date_Introduced, Fabric_Id, Product_Type_Id, CP)
                VALUES (%s, %s, (SELECT id from Fabric_Codes WHERE Fabric_Code = %s), (SELECT id from Product_Types WHERE Product_Type = %s), %s);
        """ % (
            table_name,
            sanitizeData(values['Item_Sku_Code']),
            sanitizeData(values['Date_Introduced']),
            sanitizeData(values['Fabric']),
            sanitizeData(values['Product_Type']),
            sanitizeData(values['CP']),
        )
    elif table_name == email_validation_table:
        return """
                INSERT INTO %s 
                (Email_ID, Email_Type, Validation_Date)
                VALUES (%s, %s, %s);
        """ % (
            table_name,
            sanitizeData(values['Email_ID']),
            sanitizeData(values['Email_Type']),
            sanitizeData(values['Validation_Date']),
        )
    elif table_name == order_status_table:
        return """
                INSERT INTO %s 
                (Display_Order_Code, `Pre-Dispatch_Status`, Delivery_Status, Cancelled_Reason, Channel_Name)
                VALUES (%s, %s, %s, %s, (SELECT id from Channel_List WHERE Channel_Name = %s));
        """ % (
            table_name,
            sanitizeData(values['Display_Order_Code']),
            sanitizeData(values['Pre-Dispatch_Status']),
            sanitizeData(values['Delivery_Status']),
            sanitizeData(values['Cancelled_Reason']),
            sanitizeData(values['Channel_Name'])
        )
    elif table_name in (whatsapp_failed_table, whatsapp_opened_table, whatsapp_sent_table, sms_sent_table, sms_failed_table):
        return """
                INSERT INTO %s 
                (Campaign_Name, `Date`, Notification_Mobile)
                VALUES (%s, %s, %s);
        """ % (
            table_name,
            sanitizeData(values['Campaign_Name']),
            sanitizeData(values['Date']),
            sanitizeData(values['Notification_Mobile']),
        )
    elif table_name == sms_opened_table:
        return """
                INSERT INTO %s 
                (Campaign_Name, `Date`, Notification_Mobile, browser, os)
                VALUES (%s, %s, %s, %s, %s);
        """ % (
            table_name,
            sanitizeData(values['Campaign_Name']),
            sanitizeData(values['Date']),
            sanitizeData(values['Notification_Mobile']),
            sanitizeData(values['browser']),
            sanitizeData(values['os']),
        )
    elif table_name == product_type_table:
        return """
                INSERT INTO %s 
                (Product_Type)
                VALUES (%s);
        """ % (
            table_name,
            sanitizeData(values['Product_Type']),
        )
    elif table_name == channel_list_table:
        return """
                INSERT INTO %s 
                (Channel_Name)
                VALUES (%s);
        """ % (
            table_name,
            sanitizeData(values['Channel_Name']),
        )
    elif table_name == fabric_code_table:
        return """
                INSERT INTO %s 
                (Fabric_Code)
                VALUES (%s);
        """ % (
            table_name,
            sanitizeData(values['Fabric_Codes']),
        )
    elif table_name == whatsapp:
        return """
                INSERT INTO %s 
                (Campaign_Name, `Date`, Country_Code, Notification_Mobile, Status, Reply)
                VALUES (%s);
        """ % (
            table_name,
            sanitizeData(
                sanitizeData(values['Campaign_Name']),
                sanitizeData(values['Date']),
                sanitizeData(values['Country_Code']),
                sanitizeData(values['Notification_Mobile']),
                sanitizeData(values['Status']),
                sanitizeData(values['Reply']),
            ),
        )
    elif table_name == zero_order_customers:
        return """
                INSERT INTO %s 
                (Name, Country_Code, Notification_Mobile, Notification_Email, Source, `Exists`)
                VALUES (%s, %s, %s, %s, %s, %s);
        """ % (
            table_name,
            sanitizeData(values['Name']),
            sanitizeData(values['Country_Code']),
            sanitizeData(values['Notification_Mobile']),
            sanitizeData(values['Notification_Email']),
            sanitizeData(values['Source']),
            sanitizeData(values['Exists']),
        )


def getSelectQuery(table_name):
    if table_name == order_data_table:
        return """
                SELECT od.id, 
            od.Sale_Order_Item_Code,
            od.Display_Order_Code, 
            (Select sd.Item_Sku_Code from Sku_Data sd where sd.id = od.Item_SKU_Id) as 'Item_SKU_Code',
            (Select cl.Channel_Name from Channel_List cl where cl.id = od.Channel_Id) as 'Channel_Name',
            od.Order_Date, 
            od.Notifcation_Email as Notification_Email,
            od.Notification_Mobile, 
            od.Shipping_Address_Name,
            od.Shipping_Address_Line1, 
            od.Shipping_Address_Line2,
            od.Shipping_Address_City, 
            od.Shipping_Address_State,
            od.Shipping_Address_Country, 
            od.Shipping_Address_Pincode
            FROM Order_Data od;
            """
    elif table_name == order_status_table:
        return """
            SELECT os.id, os.Display_Order_Code , os.`Pre-Dispatch_Status` , os.Cancelled_Reason , os.Delivery_Status , cl.Channel_Name 
            from Order_Status os 
            JOIN Channel_List cl 
            ON cl.id = os.Channel_Id;
        """
    elif table_name == sku_data_table:
        return """
            SELECT sku.id, sku.Item_Sku_Code, sku.Date_Introduced, 
            (select fab.Fabric_Code from Fabric_Codes fab
            where fab.id = sku.Fabric_Id) as 'Fabric_Id',
            (select fab.Product_Type from Product_Types fab
            where fab.id = sku.Product_Type_Id) as 'Product_Type_Id',
                sku.CP
            FROM Sku_Data sku;
        """
    else:
        return "SELECT * FROM {}".format(table_name)

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

update_sku_data = 'Update Sku_Data'
update_order_data = 'Update Order_Data'

zero_order_customers = 'Zero_Order_Customers'

returns_table = 'Returns'

backfill_product_id_selling_price = 'Backfill Product_ID_Selling_Price'

table_list = ['Order_Data', 'Sku_Data', 'Email_Validation', 'Order_Status',
              'Channel_List', 'Fabric_Codes', 'Product_Types', 'SMS_Failed', 'SMS_Opened',
              'SMS_Sent', 'Whatsapp_Failed', 'Whatsapp_Opened', 'Whatsapp_Sent', 'Whatsapp',
              'Zero_Order_Customers', 'Returns', 'Update Sku_Data', 'Update Order_Data',
              'Backfill Product_ID_Selling_Price']

uploadCSVFormatMap = {
    "Order_Data": [
        "Sale_Order_Item_Code",
        "Display_Order_Code",
        "Notification_Email",
        "Notification_Mobile",
        "Shipping_Address_Name",
        "Shipping_Address_Line1",
        "Shipping_Address_Line2",
        "Shipping_Address_City",
        "Shipping_Address_State",
        "Shipping_Address_Country",
        "Shipping_Address_Pincode",
        "Order_Date",
        "Channel_Name",
        "Item_SKU_Code",
        "Channel_Product_ID",
        "Selling_Price",
    ],
    'Update Order_Data': [
        "Sale_Order_Item_Code",
        "Display_Order_Code",
        "Channel_Name",
        "Notification_Email",
        "Notification_Mobile",
        "Shipping_Address_Name",
        "Shipping_Address_Line1",
        "Shipping_Address_Line2",
        "Shipping_Address_City",
        "Shipping_Address_State",
        "Shipping_Address_Country",
        "Shipping_Address_Pincode",
        "Order_Date",
        "Item_SKU_Code",
        "Channel_Product_ID",
        "Selling_Price",
    ],
    "Backfill Product_ID_Selling_Price": [
        "Channel_Product_ID",
        "Selling_Price",
        "Sale_Order_Item_Code",
        "Display_Order_Code",
        "Channel_Name",
    ],
    "Sku_Data": [
        "Item_Sku_Code",
        "Date_Introduced",
        "Fabric",
        "Product_Type",
        "CP",
    ],
    "Update Sku_Data": [
        "Item_Sku_Code",
        "Date_Introduced",
        "Fabric",
        "Product_Type",
        "CP",
    ],
    "Email_Validation": ["Email_ID", "Email_Type", "Validation_Date"],
    "Order_Status": [
        "Display_Order_Code",
        "Pre-Dispatch_Status",
        "Delivery_Status",
        "Cancelled_Reason",
        "Channel_Name",
    ],
    "Channel_List": ["Channel_Name"],
    "Fabric_Codes": ["Fabric_Code"],
    "Product_Types": ["Product_Type"],
    "SMS_Failed": ["Campaign_Name", "Date", "Notification_Mobile"],
    "SMS_Opened": [
        "Campaign_Name",
        "Date",
        "Notification_Mobile",
        "browser",
        "os",
    ],
    "SMS_Sent": ["Campaign_Name", "Date", "Notification_Mobile"],
    "Whatsapp_Failed": ["Campaign_Name", "Date", "Notification_Mobile"],
    "Whatsapp_Opened": ["Campaign_Name", "Date", "Notification_Mobile"],
    "Whatsapp_Sent": ["Campaign_Name", "Date", "Notification_Mobile"],
    "Whatsapp": [
        "Campaign_Name",
        "Date",
        "Country_Code",
        "Notification_Mobile",
        "Status",
        "Reply",
    ],
    "Zero_Order_Customers": [
        "Name",
        "Country_Code",
        "Notification_Mobile",
        "Notification_Email",
        "Source",
        "Date",
        "Channel_Name",
    ],
    "Returns": [
        "Display_Order_Code",
        "Channel_Name",
        "Sale_Order_Item_Code",
        "Channel_Product_Id"
    ]
}


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
            (Channel_Product_ID, Selling_Price, Sale_Order_Item_Code, Display_Order_Code, 
            Notifcation_Email, Notification_Mobile, Shipping_Address_Name, Shipping_Address_Line1, 
            Shipping_Address_Line2, Shipping_Address_City, Shipping_Address_State, Shipping_Address_Country, 
            Shipping_Address_Pincode, Order_Date, Channel_Id, Item_SKU_Id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (SELECT id from Channel_List WHERE Channel_Name = %s), (SELECT id from Sku_Data WHERE Item_Sku_Code = %s));
        """ % (
            table_name,
            sanitizeData(values['Channel_Product_ID']),
            float(values['Selling_Price']) if values['Selling_Price'] else "NULL",
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
                (Display_Order_Code, `Pre-Dispatch_Status`, Delivery_Status, Cancelled_Reason, Channel_Id)
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
            sanitizeData(values['Fabric_Code']),
        )
    elif table_name == whatsapp:
        return """
                INSERT INTO %s 
                (Campaign_Name, `Date`, Country_Code, Notification_Mobile, Status, Reply)
                VALUES (%s, %s, %s, %s, %s, %s);
        """ % (
            table_name,
            sanitizeData(values['Campaign_Name']),
            sanitizeData(values['Date']),
            sanitizeData(values['Country_Code']),
            sanitizeData(values['Notification_Mobile']),
            sanitizeData(values['Status']),
            sanitizeData(values['Reply']),
        )
    elif table_name == zero_order_customers:
        return """
                INSERT INTO %s 
                (Name, Country_Code, Notification_Mobile, Notification_Email, Source, Date, Channel_Id)
                VALUES (%s, %s, %s, %s, %s, %s, (SELECT id from Channel_List WHERE Channel_Name = %s));
        """ % (
            table_name,
            sanitizeData(values['Name']),
            sanitizeData(values['Country_Code']),
            sanitizeData(values['Notification_Mobile']),
            sanitizeData(values['Notification_Email']),
            sanitizeData(values['Source']),
            sanitizeData(values['Date']),
            sanitizeData(values['Channel_Name']),
        )
    elif table_name == update_sku_data:
        return """
                UPDATE Sku_Data
                SET Date_Introduced = %s,
                Fabric_Id = (SELECT id from Fabric_Codes WHERE Fabric_Code = %s),
                Product_Type_Id = (SELECT id from Product_Types WHERE Product_Type = %s),
                CP = %s
                WHERE Item_Sku_Code = %s;
        """ % (
            sanitizeData(values['Date_Introduced']),
            sanitizeData(values['Fabric']),
            sanitizeData(values['Product_Type']),
            sanitizeData(values['CP']),
            sanitizeData(values['Item_Sku_Code']),
        )
    elif table_name == update_order_data:
        return """
                UPDATE Order_Data
                SET 
                Channel_Product_ID = %s,
                Selling_Price = %s,
                Notifcation_Email = %s,
                Notification_Mobile = %s,
                Shipping_Address_Name = %s,
                Shipping_Address_Line1 = %s,
                Shipping_Address_Line2 = %s,
                Shipping_Address_City = %s,
                Shipping_Address_State = %s,
                Shipping_Address_Country = %s,
                Shipping_Address_Pincode = %s,
                Order_Date = %s,
                Item_SKU_Id = (SELECT id from Sku_Data WHERE Item_Sku_Code = %s)
                WHERE Sale_Order_Item_Code = %s AND Display_Order_Code = %s AND Channel_Id = (SELECT id from Channel_List WHERE Channel_Name = %s);
        """ % (
            sanitizeData(values['Channel_Product_ID']),
            float(values['Selling_Price']
                  ) if values['Selling_Price'] else "NULL",
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
            sanitizeData(values['Item_SKU_Code']),
            sanitizeData(values['Sale_Order_Item_Code']),
            sanitizeData(values['Display_Order_Code']),
            sanitizeData(values['Channel_Name'])
        )
    elif table_name == backfill_product_id_selling_price:
        return """
            UPDATE Order_Data 
            SET Channel_Product_ID = %s,
            Selling_Price = %s 
            WHERE Sale_Order_Item_Code = %s AND Display_Order_Code = %s AND Channel_Id = (SELECT id FROM Channel_List cl WHERE cl.Channel_Name = %s)
        """ % (
            sanitizeData(values['Channel_Product_ID']),
            float(values['Selling_Price']
                  ) if values['Selling_Price'] else "NULL",
            sanitizeData(values['Sale_Order_Item_Code']),
            sanitizeData(values['Display_Order_Code']),
            sanitizeData(values['Channel_Name'])
        )
    elif table_name == returns_table:
        return """
            INSERT INTO `Returns`
            (Display_Order_Code, Channel_Id, Sale_Order_Item_Code, Channel_Product_Id)
            VALUES
            (%s, (SELECT id FROM Channel_List cl WHERE cl.Channel_Name = %s), %s, %s);
        """ % (
            sanitizeData(values['Display_Order_Code']),
            sanitizeData(values['Channel_Name']),
            sanitizeData(values['Sale_Order_Item_Code']),
            sanitizeData(values['Channel_Product_Id']),
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
            od.Shipping_Address_Pincode,
            od.Channel_Product_ID,
            od.Selling_Price
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
    elif table_name == zero_order_customers:
        return """
            SELECT zoc.id, zoc.Name, zoc.Country_Code, zoc.Notification_Mobile, zoc.Notification_Email, zoc.Source, zoc.`Date`, zoc.Last_Order_Date,
            (SELECT Channel_Name from Channel_List cl WHERE cl.id = zoc.Channel_Id) as Channel_Name
            from Zero_Order_Customers zoc;
        """
    elif table_name == returns_table:
        return """
            SELECT rs.id, rs.Display_Order_Code, rs.Sale_Order_Item_Code, rs.Channel_Product_Id, cl.Channel_Name
            FROM `Returns` rs
            JOIN Channel_List cl 
            on cl.id = rs.Channel_Id;
        """
    else:
        return "SELECT * FROM {}".format(table_name)

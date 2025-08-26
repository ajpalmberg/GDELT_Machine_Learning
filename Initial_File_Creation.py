import pandas as pd
from datetime import datetime, timedelta


gics_keywords = {
    # 1-10
    "Oil & Gas Drilling": ["offshore rigs", "exploration wells", "drilling operations", "oilfield services", "deepwater drilling"],
    "Oil & Gas Equipment & Services": ["pipeline equipment", "refinery maintenance", "energy engineering", "oilfield tools", "extraction technology"],
    "Integrated Oil & Gas": ["oil refining", "natural gas production", "energy distribution", "crude oil reserves", "petroleum operations"],
    "Oil & Gas Exploration & Production": ["shale gas", "oil extraction", "upstream operations", "crude reserves", "drilling exploration"],
    "Oil & Gas Refining & Marketing": ["petroleum refining", "fuel distribution", "retail gasoline", "oil supply chains", "crude processing"],
    "Oil & Gas Storage & Transportation": ["crude oil pipelines", "storage terminals", "liquefied natural gas", "energy logistics", "petroleum shipping"],
    "Coal & Consumable Fuels": ["coal mining", "thermal power", "fossil fuels", "coal transportation", "energy combustion"],
    "Commodity Chemicals": ["bulk chemicals", "industrial solvents", "petrochemical products", "polymer resins", "chemical manufacturing"],
    "Specialty Chemicals": ["adhesive chemicals", "industrial coatings", "chemical additives", "specialty polymers", "high-performance pigments"],
    "Industrial Gases": ["liquid nitrogen", "hydrogen production", "gas cylinders", "carbon dioxide supply", "industrial oxygen"],

    # 11-20
    "Fertilizers & Agricultural Chemicals": ["crop fertilizers", "pesticide solutions", "soil treatment", "nitrogen products", "agricultural growth"],
    "Construction Materials": ["ready-mix concrete", "asphalt paving", "infrastructure cement", "construction aggregates", "building materials"],
    "Metal & Glass Containers": ["beverage cans", "food packaging glass", "industrial containers", "metal bottle caps", "recyclable packaging"],
    "Paper Packaging": ["cardboard boxes", "corrugated packaging", "recycled paper products", "shipping cartons", "industrial packaging"],
    "Diversified Metals & Mining": ["metal ore extraction", "copper mining", "iron production", "metal alloys", "global mining"],
    "Gold": ["gold bullion", "precious metal mining", "jewelry gold", "gold reserves", "investment gold"],
    "Steel": ["structural steel", "alloy production", "steel manufacturing", "construction steel", "industrial steel sheets"],
    "Precious Metals & Minerals": ["platinum mining", "silver extraction", "jewelry metals", "rare minerals", "industrial platinum"],
    "Paper Products": ["tissue paper", "printing stock", "paperboard products", "office paper supplies", "pulp production"],
    "Forest Products": ["lumber processing", "wood harvesting", "timber exports", "plywood production", "forestry management"],

    # 21-30
    "Aerospace & Defense": ["military aircraft", "defense contracts", "satellite technology", "space systems", "missile production"],
    "Building Products": ["HVAC systems", "insulation materials", "construction fixtures", "window manufacturing", "door hardware"],
    "Construction & Engineering": ["civil engineering", "construction projects", "infrastructure building", "contracting services", "industrial construction"],
    "Electrical Equipment": ["power transformers", "industrial switches", "electrical motors", "circuit breakers", "energy components"],
    "Industrial Conglomerates": ["diversified holdings", "global industrial operations", "multi-sector manufacturing", "industrial investments", "cross-industry production"],
    "Machinery": ["industrial automation", "engine manufacturing", "heavy machinery", "robotics systems", "manufacturing tools"],
    "Trading Companies & Distributors": ["supply chain management", "wholesale goods", "logistics solutions", "global trade services", "industrial distribution"],
    "Commercial Printing": ["printing presses", "promotional materials", "graphic brochures", "industrial publishing", "advertising print"],
    "Environmental & Facilities Services": ["waste recycling", "environmental cleanup", "industrial waste management", "facility maintenance", "sustainability solutions"],
    "Office Services & Supplies": ["business stationery", "office printers", "corporate furniture", "office ink supplies", "document shredding"],

    # 31-40
    "Security & Alarm Services": ["home surveillance", "business alarms", "security monitoring", "access control systems", "video surveillance"],
    "Diversified Support Services": ["business consulting", "corporate staffing", "facilities outsourcing", "logistics support", "customized solutions"],
    "Air Freight & Logistics": ["cargo planes", "freight transport", "global shipping", "air logistics", "express delivery"],
    "Airlines": ["passenger airlines", "commercial aviation", "flight services", "airline fleets", "air travel operations"],
    "Marine": ["shipping vessels", "freight containers", "maritime logistics", "port management", "ocean freight"],
    "Road & Rail": ["rail freight services", "trucking logistics", "railway infrastructure", "cargo transport", "road shipping"],
    "Transportation Infrastructure": ["airport construction", "bridge engineering", "port facilities", "highway projects", "public transit systems"],
    "Auto Parts & Equipment": ["automotive brakes", "engine components", "vehicle transmissions", "tire manufacturing", "automobile parts"],
    "Automobiles": ["electric vehicles", "car manufacturing", "luxury autos", "vehicle assembly", "automobile brands"],
    "Household Durables": ["home appliances", "residential furniture", "consumer electronics", "kitchen equipment", "home fixtures"],

    # 41-50
    "Leisure Products": ["sports equipment", "gaming consoles", "hobby kits", "outdoor recreation", "toy manufacturing"],
    "Textiles, Apparel & Luxury Goods": ["designer fashion", "luxury accessories", "footwear production", "premium textiles", "branded apparel"],
    "Hotels, Resorts & Cruise Lines": ["luxury hotels", "vacation packages", "cruise ships", "resort services", "travel bookings"],
    "Restaurants": ["casual dining", "fast food chains", "restaurant franchises", "culinary services", "food delivery"],
    "Leisure Facilities": ["fitness clubs", "golf courses", "amusement parks", "recreation centers", "sports facilities"],
    "Casinos & Gaming": ["online gambling", "casino resorts", "lottery operations", "slot machines", "sports betting"],
    "Distributors": ["product delivery", "wholesale services", "logistics distribution", "supply chains", "industrial distribution"],
    "Internet & Direct Marketing Retail": ["e-commerce platforms", "online retail", "digital shopping", "direct-to-consumer", "online marketplaces"],
    "Multiline Retail": ["department stores", "retail chains", "consumer shopping", "multi-category retail", "general merchandise"],
    "Specialty Retail": ["niche stores", "specialized products", "luxury retail", "boutique services", "focused markets"],

    # 51-60
    "Drug Retail": ["pharmacy chains", "prescription services", "healthcare products", "drugstore brands", "wellness retail"],
    "Food Distributors": ["grocery suppliers", "food logistics", "restaurant supplies", "wholesale food", "supermarket chains"],
    "Food Retail": ["grocery stores", "food chains", "consumer food", "supermarkets", "fresh produce"],
    "Beverages": ["alcohol brands", "soft drinks", "bottling facilities", "juice production", "beverage marketing"],
    "Food Products": ["snack manufacturing", "meat processing", "frozen foods", "dairy products", "ingredient suppliers"],
    "Tobacco": ["cigarette brands", "vape products", "cigar manufacturing", "nicotine solutions", "smoking accessories"],
    "Household Products": ["cleaning supplies", "detergent brands", "home care products", "sanitation items", "household goods"],
    "Personal Products": ["cosmetic brands", "skincare solutions", "hygiene products", "personal care items", "beauty products"],
    "Health Care Equipment": ["medical devices", "diagnostic equipment", "surgical instruments", "patient monitors", "healthcare technology"],
    "Health Care Supplies": ["single-use gloves", "testing kits", "bandages", "medical consumables", "clinical supplies"],

    # 61-69
    "Health Care Providers & Services": ["hospital management", "health insurance", "clinic operations", "care facilities", "medical services"],
    "Health Care Technology": ["telemedicine platforms", "healthcare software", "medical data analytics", "health IT solutions", "digital health"],
    "Biotechnology": ["gene therapy", "biopharmaceuticals", "genomics research", "drug development", "biotech innovation"],
    "Pharmaceuticals": ["drug manufacturing", "medicinal production", "vaccine development", "pharmaceutical research", "clinical trials"],
    "Life Sciences Tools & Services": ["genetics equipment", "laboratory instruments", "scientific research tools", "analytical services", "biotech tools"],
    "Insurance Brokers": ["policy management", "insurance advisory", "risk assessment services", "broker commissions", "insurance markets"],
    "Reinsurance": ["risk transfer", "insurance underwriting", "catastrophic coverage", "reinsurance treaties", "global reinsurers"],
    "Real Estate Services": ["property management", "real estate advisory", "brokerage services", "commercial leasing", "market appraisals"],
    "Health Care Distributors": ["pharmaceutical logistics", "medical supply chain", "healthcare wholesale", "drug distribution", "clinical product delivery"]
}

companies = [
    'Nvidia Corp', 'Microsoft Corp', 'Amazon', 'Facebook', 'Alphabet', 'Tesla', 'Berkshire Hathaway', 'Google', 'Broadcom', 'Jpmorgan Chase',
    'Eli Lilly & Co.', 'Unitedhealth Group', 'Exxon Mobil', 'Visa', 'Mastercard', 'Costco', 'Home Depot', 'Procter & Gamble', 'Johnson & Johnson', 'Walmart Inc.',
    'Netflix Inc', 'Bank of America', 'Salesforce', 'Oracle Corp', 'Abbvie Inc.', 'Chevron', 'Wells Fargo', 'Merck & Co.', 'Coca-Cola', 'Cisco Systems',
    'Advanced Micro Devices', 'Adobe', 'Accenture Plc', 'Pepsi.', 'Linde Plc', "Mcdonald's", 'Servicenow', 'Disney', 'Philip Morris International', 'Abbott Laboratories',
    'Ge Aerospace', 'International Business Machines', 'Thermo Fisher Scientific', 'Intuit', 'Texas Instruments', 'Intuitive Surgical', 'Caterpillar', 'Goldman Sachs', 'Qualcomm Inc',
    'Verizon Communications', 'Booking Holdings', 'Morgan Stanley', 'Comcast Corp', 'At&t Inc.', 'S&p Global Inc.', 'American Express Company', 'Rtx Corporation', 'Nextra Energy, Inc.', "Lowe's Companies Inc.", 
    'Progressive Corporation', 'Amgen Inc', 'Honeywell International, Inc.', 'Danaher Corporation', 'Blackrock, Inc.', 'Uber Technologies, Inc.', 'Union Pacific Corp.', 'Eaton Corporation, Plcs', 'Applied Materials Inc', 
    'Pfizer Inc.', 'Tjx Companies, Inc.', 'Stryker Corporation', 'Boston Scientific Corp.', 'Conocophillips', 'Blackstone Inc.', 'Citigroup Inc.', 'Palo Alto Networks, Inc.', 'Palantir Technologies Inc. Class A', 'Fiserv, Inc.',
    'The Charles Schwab Corporation', 'Automatic Data Processing', 'Vertex Pharmaceuticals Inc', 'T-Mobile Us, Inc.', 'Bristol-Myers Squibb Co.', 'Starbucks Corp', 'Lockheed Martin Corp.', 'Medtronic Plc', 'Gilead Sciences Inc', 
    'Marsh & Mclennan Companies, Inc.', 'Chubb Limited', 'Micron Technology, Inc.', 'Prologis, Inc.', 'Intel Corp', 'Boeing Company', 'Analog Devices, Inc.', 'Deere & Company', 'Kkr & Co. Inc.', 'United Parcel Service, Inc. Class B',
    'The Southern Company', 'Altria Group, Inc.', 'Arista Networks', 'Elevance Health, Inc.', 'American Tower Corporation', 'Trane Technologies Plc', 'Lam Research Corp', 'Intercontinental Exchange Inc.', 'The Cigna Group', 'Nike, Inc.',
    'Ge Vernova Inc.', 'Parker-Hannifin Corporation', 'Duke Energy Corporation', 'The Sherwin-Williams Company', 'Paypal Holdings, Inc.', 'Equinix, Inc. Reit', 'Mondelez International, Inc. Class A', 'Amphenol Corporation', 'Kla Corporation', 
    'Aon Plc Class A', 'Pnc Financial Services Group', 'Cme Group Inc.', 'Motorola Solutions, Inc.', 'Regeneron Pharmaceuticals Inc', 'Cadence Design Systems', 'Chipotle Mexican Grill, Inc.', 'Synopsys Inc', 'Zoetis Inc.', 'Mckesson Corporation',
    'Waste Management, Inc.', 'Welltower Inc.', 'Crowdstrike Holdings, Inc. Class A', 'U.S. Bancorp', 'Eog Resources, Inc.', 'Colgate-Palmolive Company', 'Cintas Corp', 'Emerson Electric Co.', "Moody's Corporation", 'General Dynamics Corporation',
    'Illinois Tool Works Inc.', 'Air Products & Chemicals, Inc.', 'Target Corporation', 'Constellation Energy Corporation', '3m Company', "O'reilly Automotive, Inc.", 'Williams Companies Inc.', 'Cvs Health Corporation', 'Transdigm Group Incorporated', 'Capital One Financial',
    'Csx Corporation', 'Northrop Grumman Corp.', 'Fedex Corporation', 'Marriot International Class A', 'Oneok, Inc.', 'Autodesk Inc', 'Becton, Dickinson and Co.', 'Arthur J. Gallagher & Co.', 'General Motors Company', 'Truist Financial Corporation',
    'Freeport-Mcmoran Inc.', 'Hca Healthcare, Inc.', 'Ecolab, Inc.', 'Carrier Global Corporation', 'Schlumberger Limited', 'Hilton Worldwide Holdings Inc.', 'The Travelers Companies, Inc.', 'Norfolk Southern Corp.', 'Roper Technologies, Inc.', 'Sempra',
    'Paccar Inc', 'Fortinet, Inc.', 'Airbnb, Inc. Class A', 'Bank of New York Mellon Corporation', 'Aflac Inc.', 'Digital Realty Trust, Inc.', 'Nxp Semiconductors N.v.', 'Johnson Controls International Plc', 'Fair Isaac Corporation', 'Ameriprise Financial, Inc.',
    'United Rentals, Inc.', 'Simon Property Group, Inc.', 'Phillips 66', 'Royal Caribbean Group', 'Autozone, Inc.', 'Kinder Morgan, Inc.', 'Marathon Petroleum Corporation', 'Public Storage', 'The Allstate Corporation', 'W.W. Grainger, Inc.',
    'American Electric Power Company, Inc.', 'Vistra Corp.', 'Cummins Inc.', 'Copart Inc', 'Realty Income Corporation', 'American International Group, Inc.', 'Metlife, Inc.', 'Newmont Corporation', 'Dominion Energy, Inc.', 'Quanta Services, Inc.',
    'Fidelity National Information Services, Inc.', 'D.R. Horton Inc.', 'Ross Stores Inc', 'Fastenal Co', 'Msci, Inc.', 'L3harris Technologies, Inc.', 'Howmet Aerospace Inc.', 'Valero Energy Corporation', 'Kimberly-Clark Corp.', 'Crown Castle Inc.',
    'Paychex Inc', 'Kenvue Inc.', 'Prudential Financial, Inc.', 'Te Connectivity Ltd', 'Pg&e Corporation', 'Ametek, Inc.', 'Public Service Enterprise Group Incorporated', 'Ford Motor Company', 'Targa Resources Corp.', 'Discover Financial Services',
    'Axon Enterprise, Inc.', 'Baker Hughes Company', 'Republic Services Inc.', 'Cencora, Inc.', 'Edwards Lifesciences Corp', 'Ingersoll Rand Inc.', 'Hess Corporation', 'Lennar Corporation Class A', 'Delta Air Lines, Inc.', 'Otis Worldwide Corporation',
    'Old Dominion Freight Line', 'Gartner, Inc.', 'Cbre Group, Inc.', 'Verisk Analytics, Inc.', 'Corteva, Inc.', 'Electronic Arts Inc', 'Exelon Corporation', 'Dell Technologies Inc.', 'Xcel Energy, Inc.', 'The Kroger Co.',
    'Cognizant Technology Solutions', 'Ge Healthcare Technologies Inc.', 'Yum! Brands, Inc.', 'Monster Beverage Corporation', 'Charter Comm Inc Del Cl a', 'Constellation Brands, Inc.', 'Sysco Corporation', 'Agilent Technologies Inc.', 'Vulcan Materials Company', 'Hp Inc.',
    'Arch Capital Group Ltd', 'M&t Bank Corp.', 'Corning Incorporated', 'Lululemon Athletica Inc.', 'Martin Marietta Materials', 'Resmed Inc.', 'Keurig Dr Pepper Inc.', 'Microchip Technology Inc', 'General Mills, Inc.', 'Nucor Corporation',
    'Extra Space Storage, Inc.', 'The Hartford Financial Services Group, Inc.', 'Idexx Laboratories Inc', 'Occidental Petroleum Corporation', 'Wabtec Inc.', 'Dupont De Nemours, Inc.', 'Diamondback Energy, Inc.', 'Iron Mountain Inc.', 'Vici Properties Inc.', 'Consolidated Edison, Inc.',
    'Iqvia Holdings Inc.', 'Nasdaq, Inc.', 'Humana Inc.', 'Avalonbay Communities, Inc.', 'Edison International', 'Garmin Ltd', 'Rockwell Automation, Inc.', 'Entergy Corporation', 'Willis Towers Watson Public Limited Companys', 'Fifth Third Bancorp',
    'Wec Energy Group, Inc.', 'Dow Inc.', 'Centene Corporation', 'Dexcom, Inc.', 'Equifax, Incorporated', 'Raymond James Financial, Inc.', 'Costar Group Inc', 'Ebay Inc', 'Tractor Supply Co', 'Take-Two Interactive Software Inc',
    'Xylem Inc', 'United Airlines Holdings, Inc.', 'Global Payments, Inc.', 'Ansys Inc', 'Cardinal Health, Inc.', 'Ppg Industries, Inc.', 'State Street Corporation', 'Monolithic Power Systems, Inc.', 'On Semiconductor Corp', 'Hewlett Packard Enterprise Company',
    'Dover Corporation', 'The Kraft Heinz Company', 'Nvr, Inc.', 'Church & Dwight Co., Inc.', 'Deckers Outdoor Corp', 'Halliburton Company', 'Pultegroup, Inc.', 'American Water Works Company, Inc', 'Broadridge Financial Solutions Inc', 'Keysight Technologies, Inc.',
    'T Rowe Price Group Inc', 'Ventas, Inc.', 'Smurfit Westrock Plc', 'Eqt Corp', 'Fortive Corporation', 'Godaddy Inc', 'Brown & Brown, Inc.', 'Huntington Bancshares Inc', 'Tyler Technologies, Inc.', 'Veralto Corporation',
    'Equity Residential', 'Corpay, Inc.', 'Synchrony Financial', 'Archer Daniels Midland Company', 'The Hershey Company', 'Ppl Corporation', 'Dte Energy Company', 'Mettler-Toledo International', 'Carnival Corporation', 'Ameren Corporation'
]

def get_dates_in_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)
    
    return date_list

# Preparing the list of keywords (using gics_keywords and companies)
all_keywords = []
for keywords in gics_keywords.values():
    all_keywords.extend(keywords)
for word in companies:
    all_keywords.append(word)

# Generate the keyword columns for Volume and AvgSentiment
keyword_columns = []
for word in all_keywords:
    keyword_columns.extend([f"{word}_Volume", f"{word}_AvgSentiment"])

# Define the columns of the final DataFrame, adding the "Complete" column as the second column
columns = ["Date", "Complete"] + keyword_columns

# Generate the dates for the given range
dates = get_dates_in_range('20130401', '20241114')
dates.reverse()  # Reverse the dates list

# Create rows with the date, empty "Complete" column, and None for keyword columns
rows = []
for date in dates:
    row = [date, None] + [None] * len(keyword_columns)  # Empty value for "Complete"
    rows.append(row)

# Create the master DataFrame
master_csv = pd.DataFrame(rows, columns=columns)

# Save to CSV file
master_csv.to_csv(r"C:\Users\Palmbera\Downloads\TempVS\temp.csv", index=False)

print("CSV created successfully with 'Complete' column as the second column.")
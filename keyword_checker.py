from goose3 import Goose
from newspaper import Article
from newspaper.article import ArticleException
import pandas as pd
import os
import time
import random
import requests
from urllib.parse import urlparse
import socket

master_keywords_list = ['case law', 'governance', 'f&b', 'cloud technology', 'political ideologies', 'industrial manufacturing', 'international diplomacy', 'internet retail', 'independents', 'berkshire', 'media', 'computer networking', 'tax treaties', 'entertainment', 'multilateral diplomacy', 'wholesale trade', 'court rulings', 'robotic engineering', 'nvidia corp', 'credit cards', 'consumer electronics', 'coca-cola', 'enterprise software', 'legal advocacy', 'capital gains tax', 'facebook', 'political corruption', 'socialism', 'intuitive surgical inc', 'veterans', 'the home depot', 'voting districts', 'weaponry', 'energy sector', 'electronics industry', 'government', 'military alliances', 'lawsuit settlements', 'election interference', 'showbiz', 'social networks', 'electoral vote', 'disarmament', 'social platforms', 'military law', 'squadron', 'automotive', 'mobil', 'american express company', 'trade agreements', 'telecom', 'lobbying', 'intuitive surgical', 'payment systems', 'right-wing', 'business software', 'auto insurance', 'combat', 'texas instruments inc', 'costco wholesale corporation', 'servicenow', 'morgan stanley & co', 'abbott labs', 'amazon.com', 'morgan stanley', 'legal briefings', 'payments', 'defense', 'anarchy', 'gerrymandering', 'supreme court', 'army recruits', 'export tariffs', 'broadcom', 'house of representatives', 'executive orders', 
'walmart', 'p&g', 'philip morris international', 'advanced micro devices inc', 'socialists', 'army reserves', 'food and beverage', 'national guard', 'finance', 'merck', 'health insurance', 'visa', 'thermo fisher scientific', 'alphabet', 'electronic devices', 'factory production', 'summits', 'soldiers', 'war crimes', 'air force', 'bar association', 'cloud infrastructure', 'united nations', 'special forces', 'coca cola', 'banking', 'judicial appointments', 'homedepot', 'machine learning', 'auto manufacturing', 'insurgency', 'military operations', 'wal-mart', 'tesla inc', 'insurance industry', 'ecommerce industry', 'troop deployment', 'taxation', 'civil rights', "mcdonald's corporation", 'communism', 'online retail', 'law enforcement', 'regulations', 'procter and gamble', 'regressive tax', 'global governance', 'embassy staff', 'income tax', 'plastic cards', 'litigation', 'fighter jets', 'movie industry', 'exxon mobil', 'military coup', 'tech gadgets', 'tax audits', 'amazon', 'general electric', 'legal counsel', 'meta platforms inc', 'state law', 'lilly', 'eli lilly', 'diplomatic failure', 'broadcom inc', 'social networking', 'diplomacy', 'jp morgan chase', 'civil law', 'world trade organization', 'financial services', 'legal procedures', 'lawsuits', 'servicenow inc', 'netflix inc', 'centre-left', 'salesforce.com', 'adobe systems', 'autocracy', 'adobe', 'payroll tax', 'peacekeeping', 'ambassador', ' un ', 'beverage industry', 'international treaties', 'telecom services', 'peace talks', 'bofa', 'vat', 'crm solutions', 'exxon', 'bilateral relations', 'home electronics', 'legal interpretations', 'civil war', 'republicans', 'alphabet inc', 'qualcomm', 'political leadership', 'internal revenue service', 'cloud computing', 'non-governmental organizations', 'weapon systems', 'general electric company', 'criminal law', 'corporate tax', 'mastercard', 'vehicle production', 'jpmorgan', 'bills of law', 'constitutional law', 'wholesale market', 'military tactics', 'ai ', 'corporate tax avoidance', 'food industry', 'deep learning', 'political campaigns', 'congress', 'chips', 'caucus', 'enterprise solutions', 'wal-mart stores', 'legal framework', 'arms control', 'division', 'legislative', 'lawmaking', 'microsoft', 'political scandals', 'foreign ministers', 'international taxes', 't-mobile us', 'international conferences', 'qualcomm incorporated', 'business applications', 'regiment', 'oracle corporation', 'taxes', 'salesforce inc', 'customer engagement', 'submarines', 'eli lilly and company', 'home depot', 'battalion', 'bulk distribution', 'tanks', 'war', 'legislation', 'congressional bills', 'defense department', 'orcl', 'chevron corporation', 'partisanship', 'car manufacturing', 'green party', 'tariffs', 'coverage', 'caterpillar', 'libertarians', 'navy', 'chip manufacturing', 'judicial system', 'exxonmobil', 'film & tv', 'government systems', 'caterpillar inc', 'legal arguments', ' crm ', 'humanitarian aid', 'pepsico', 'j&j', 'telecommunication networks', 'ai technology', 'tax returns', 'manufacturing', 'intuit inc', 'ngos', 'withholding tax', 'manufacturing industry', 'cisco systems', 'aircraft carriers', 'apple inc', 'armed conflict', 'statutes', 'court decisions', 'legislative bodies', 'un peacekeepers', 'oracle', 'walt disney', 'tax refunds', 'diplomatic mission', 'apple incorporated', 'the coca-cola company', 'united nations security council', 'philip morris', 'thermo fisher', 'tax breaks', 'social security taxes', 'digital commerce', 'naval ships', 'international business machines', 'mastercard inc', 'public debt', 'digital payments', 'wireless communications', 'investment services', 'proctor and gamble', 'it networking', 't-mobile', 'military spending', 'international relations', 'robotics', 'jpm', 'tesla', 'peacekeeping missions', 'army', 'merck & co', 'laws', 'e-commerce', 'lobbyists', 'electoral process', 'political movements', 'court cases', 'nflx', 'energy', 'tax incentives', 'social media', 'coca-cola company', 'security forces', 'microsoft corporation', 'advanced micro devices', 'senate', 'totalitarianism', 'bipartisanship', 'amazon.com inc', 'electronic payments', 'military justice', 'military strategy', 'pepsico inc', 'wells fargo & company', 'united health', 'bills of rights', 'telecommunications', 'legal system', 'international tax law', 'the walt disney company', 'troops', 'wealth tax', 'military intervention', 'unitedhealth group', 'cisco systems inc', 'legislative process', 'apple', 'conflict', 'warfare', 'legislative reforms', 'foreign policy', 'multinational organizations', 'industrial robotics', 'campaign finance', 'credit card services', 'procter & gamble', ' ai ', 'political parties', 'missiles', 'authoritarian', 'liberalism', 'nvidia', 'texas instruments', 'election fraud', 'consul', 'flat tax', 'tax compliance', 'tax credits', 'wealth management', ' ti ', 'weapons', 'national security', 'bombers', 'johnson and johnson', 'democracy', 'military drones', 'progressive tax', 'military budget', 'tax policies', 'jpmorgan chase', 'google', 'life insurance', 'tax law', 'microchips', 'voting rights', 'fiscal deficit', 'import tariffs', 'power', 'foreign aid', 'wholesale', 'arms race', 'global partnerships', 'robotic systems', 'human rights', 'msft', 'cloud services', 'abbott laboratories', 'trade barriers', 'political rallies', 'salesforce', 'value-added tax', 'voter turnout', 'public policy', 'erp systems', 'card payments', 
'lawyers', 'wells fargo', 'diplomatic relations', 'thermofisher scientific', 'irs', 'food & drink', 'chevron', 'nvidia corporation', 'tmobile', 'crisis management', 'marines', 'fossil fuels', 'goldman sachs', 'meta', 'pmi ', 'federal law', 'coast guard', 'automation', 'abbott', ' ge ', 'military dictatorship', 'boa ', 'cloud hosting', 'goldman', 'goldman sachs group', 'parliament', 'automobile industry', 'networking', 'nato', 'bank of america corporation', 'centre-right', 'tax cuts', 'politics', 'conservatism', 'embassy', 'tea party', 'law enforcement agencies', 'google llc', 'cultural exchange', 'pepsi', 'conflict resolution', 'legal reforms', 'tax filings', 'johnson & johnson', 'meta platforms', 'customer relationship management', 'costco', 'abbvie', 'data networking', 'customs', 'american express', 'tsla', 'the goldman sachs group', 'adobe inc', 'renewable energy', 'munitions', 'electoral college', 'moderates', 'payment cards', 'amex ', 'humanitarian efforts', 'merck and co', 'united health group', 'bank of america', 'bills', 'disney', 'legislative hearings', 'tax rates', 'tmobile us', 'ibm', 'cabinet', 'semiconductors', 'berkshire hathaway inc', 'diplomatic negotiations', 'electronics', 'tax reform', 'fiscal policy', 'production', 'sanctions', 'armed forces', 'military bases', 'client management', 'online social networks', "mcdonald's", ' war ', 'armament', 'online payments', 'judicial review', 'global relations', 'lawmakers', 'broadcom corporation', 'insurance', 'artificial intelligence', 'revenue generation', 'democrats', 'democratic system', 'left-wing', 'pentagon', 'combat zones', 'amazon incorporated', 'netflix', 'customs duties', 'consumer tech', 'monarchy', 'tesla motors', 'visa inc', 'diplomatic crisis', 'sales tax', 'tax evasion', 'costco wholesale', 'jp morgan', 'military', 'abbvie inc', ' un ', 'wholesale distribution', 'mcdonalds', 'tax loopholes', 'republic', 'berkshire hathaway', 'tax avoidance', 'consulate', 'peace agreements', 'diplomatic immunity', 'crm ', 'court precedents', 'trade sanctions', 'judiciary', 'international law', 'amd ', 'property tax', 'cisco', 'unitedhealth', 'international business machines corporation', 'telecommunications network', 'silicon chips', 'intuit', 'political spectrum']

input_folder = r'Q:\Project_BOLT\Data_Storage\GDELT_News_Filtered_To_10_Countries'
output_folder = r'Q:\Project_BOLT\Data_Storage\GDELT_News_Article_Analyzed'

blacklist_file = 'blacklist.txt'


def load_blacklist():
    if os.path.exists(blacklist_file):
        with open(blacklist_file, 'r') as f:
            return set(line.strip().lower() for line in f.readlines())
    return set()


def save_blacklist(blacklist):
    with open(blacklist_file, 'w') as f:
        for domain in blacklist:
            f.write(domain + '\n')


blacklisted_domains = load_blacklist()


def extract_domain(url):
    """Extract domain from a URL, stripping 'http://www.' and everything after the first period."""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain.split('.')[0]  # Only return the first part of the domain


def fetch_and_parse(url):
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.64'
    ]
    
    headers = {
        'User-Agent': random.choice(user_agents)
    }

    try:
        # Set a timeout of 1.5 seconds
        response = requests.get(url, headers=headers, timeout=1.5)
        
        # If the server returns a 403 Forbidden error, blacklist the domain
        if response.status_code in [403, 401, 404, 410, 500, 502, 503, 504, 407]:
            print(f"403 Forbidden: {url} - Adding to blacklist")
            domain = extract_domain(url)
            blacklisted_domains.add(domain)
            save_blacklist(blacklisted_domains)
            return None  # Skip processing this URL
        
        # Raise error for any status code that isn't 2xx
        response.raise_for_status()

        # Parse the article using Newspaper3k
        article = Article(url)
        article.download()
        article.parse()

        # If the article's text is still empty, print a warning
        if not article.text:
            print(f"Warning: No text found for URL {url}")

        return article.text
    except socket.gaierror as e:
        if 'getaddrinfo failed' in str(e):
            print(f"DNS resolution error (getaddrinfo failed) for URL {url}. Skipping and adding to blacklist...")
            # Extract the domain and blacklist it
            domain = extract_domain(url)
            blacklisted_domains.add(domain)
            save_blacklist(blacklisted_domains)
        return None
    except requests.exceptions.Timeout:
        print(f"Timeout occurred for URL {url}. Skipping...")
        return None
    except requests.exceptions.RequestException as e:
        # Handle any other errors from the requests library
        print(f"Error processing URL {url}: {e}")
        return None
    except ArticleException as e:
        # Handle errors from the Newspaper library
        print(f"Error parsing article from URL {url}: {e}")
        return None


def process_csv(file_name):
    file_path = os.path.join(input_folder, file_name)
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_name}")
        return

    keywords = master_keywords_list

    # Read the input CSV file
    df = pd.read_csv(file_path, usecols=[57], skiprows=1)
    df['keywords'] = None

    # Initialize a list to hold the rows of the output CSV
    chunk_size = 1000
    chunk_count = 1  # Track the chunk number

    # Create a folder for the original CSV file in the output folder
    output_folder_for_file = os.path.join(output_folder, os.path.splitext(file_name)[0])
    os.makedirs(output_folder_for_file, exist_ok=True)

    for index, row in df.iterrows():
        key_words_in_text = []
        url = row[0]

        # Skip blacklisted URLs
        if isinstance(url, str):
            domain = extract_domain(url)
            if domain in blacklisted_domains:
                print(f"Skipping blacklisted URL: {url}")
                continue

        # Process the URL and get text if it's valid
        if isinstance(url, str) and url.startswith('http'):
            text = fetch_and_parse(url)
            if text:
                print(text[:50])  # Print a preview of the text
                for keyword in keywords:
                    count = text.lower().count(keyword.lower())
                    key_words_in_text.extend([keyword] * count)

                df.at[index, 'keywords'] = ', '.join(key_words_in_text)
                print(key_words_in_text)

        print(f"Processed {index + 1}/{len(df)} URLs")

        # Split DataFrame into chunks of 1000 rows
        if (index + 1) % chunk_size == 0 or (index + 1) == len(df):
            chunk_file_name = f"{os.path.splitext(file_name)[0]}_chunk{chunk_count}.csv"
            chunk_file_path = os.path.join(output_folder_for_file, chunk_file_name)

            # Save the current chunk to a new CSV file
            df.iloc[index - chunk_size + 1:index + 1].to_csv(chunk_file_path, index=False)
            print(f"Chunk {chunk_count} saved to {chunk_file_path}")

            chunk_count += 1  # Increment chunk counter

    print(f"All chunks saved in folder: {output_folder_for_file}")


process_csv(r'20130401.csv')
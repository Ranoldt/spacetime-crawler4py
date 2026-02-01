import re
from urllib.parse import urlparse
from typing import Iterable, Tuple
from bs4 import BeautifulSoup


MAX_HTML_BYTES = 5000000
MAX_SIGNATURE_REPEATS = 10
MIN_WORDS = 50

# To count the signature for similarity of pages
signature_counts = {}

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    raw_response = resp.raw_response
    if not raw_response: 
        return []
    html = raw_response.content
    if not html: 
        return []
    soup = BeautifulSoup(html, 'lxml')
    urls = []
    for link in soup.find_all('a'):
        # TODO: defragment url
        urls.append(link.get('href'))
        # print(link.get('href'))

    # if not html:
    #     return []
    
    # if len(html) > MAX_HTML_BYTES: # Determine very large files
    #     return []
    
    # word_count = len(words)   # To determine the information

    # signature = " ".join(words[:200])
    # if similarity_compare(signature): # Build similarity signature/report
    #     return []               

    # if word_count < MIN_WORDS:
    #     return []
    
    return urls

# O(n) where n is the length of the token string
def format_alphanum(token):
    current = -1
    formatted_token = []
    for i in range(len(token)):
        if not token[i].isalnum():
            formatted_token.append(token[current+1:i].lower())
            current = i
    formatted_token.append(token[current+1:].lower())
    return formatted_token

def extract_text(html: bytes) -> Iterable[Tuple[str, str]]:
    # creates a BeautifulSoup object that helps parse html beautifully
    # make sure to run {pip install beautifulsoup4}
    parser = BeautifulSoup(html, "html.parser")
    # we parse through each item in the html

    do_not_parse = {'style', 'title', '[document]', 'script', 'meta', 'head'}

    for item in parser.body.find_all(True):
        # ensures that we DO NOT PARSE through potential style objects or javascript
        if item.name in do_not_parse:
            continue
        # we will only parse text from the parent once bc of recursive=False
        text = "".join(item.find_all(string=True, recursive=False)).strip()
        if (item.name == 'a' and item.get("href")):
            yield item.get("href"), "URL"
        if (text):
            # split text into tokens (split on whitespace)
            tokens = format_alphanum(text)
            # list to store formatted tokens where token is first converted to lowercase then made into a Token object            
            for token in tokens:
                if token:
                    yield token, "word"
    # Parses the html
    # Yields a stream of tokens of either words or URLS with an identifier constructed as Tuple
    # EX: ("hello", "word"), ("www..ics.uci.edu/", "URL")
    

def similarity_compare(signature: str)-> bool:
    # Stores signatures of Pages into a Dictionary
    # If signature count reaches threshold, Don't extract URL from page
    if signature in signature_counts:
        signature_counts[signature] += 1
    else:
        signature_counts[signature] = 1
    
    return signature_counts[signature] > MAX_SIGNATURE_REPEATS

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if "ics.uci.edu" not in parsed.netloc and "cs.uci.edu" not in parsed.netloc and "informatics.uci.edu" not in parsed.netloc and "stat.uci.edu" not in parsed.netloc: 
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise


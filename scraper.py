import re
from urllib.parse import urlparse
from typing import Iterable, Tuple


MAX_HTML_BYTES = 5_000_000
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
    html = resp.raw_response.content
    if not html:
        return []
    
    if len(html) > MAX_HTML_BYTES: # Determine very large files
        return []
    
    tokens = extract_text(url, html)

    words, urls = [], []
    for v, t in tokens:
        if t == "word": words.append(v)
        elif t == "URL": urls.append(v)
    
    word_count = len(words)   # To determine the information

    signature = " ".join(words[:200])
    if similarity_compare(signature): # Build similarity signature/report
        return []               

    if word_count < MIN_WORDS:
        return []
    
    return urls



def extract_text(url: str, html: bytes) -> Iterable[Tuple[str, str]]:
    # Parses the html
    # Yields a stream of tokens of either words or URLS with an identifier constructed as Tuple
    # EX: ("hello", "word"), ("www..ics.uci.edu/", "URL")
    raise NotImplementedError

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

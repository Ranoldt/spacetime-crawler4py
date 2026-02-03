import re
from urllib.parse import urlparse, urljoin, urldefrag
from typing import Iterable, Tuple
from bs4 import BeautifulSoup
import hashlib


MAX_HTML_BYTES = 5000000
MAX_SIGNATURE_REPEATS = 10
MIN_WORDS = 50
HAMMING_THRESH = 5

# To store the signature for similarity of pages we have already accepted
seen_signature = []

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
    
    if len(html) > MAX_HTML_BYTES: # Determine very large files
        return []

    urls = []
    
    words = []
    urls = []
    seen_links = set()

    base = getattr(resp, "url", None) or url #handles redirects
    for tok, kind in extract_text(html, base):
        if kind == "word":
            words.append(tok)
        elif kind == "URL":
            if tok not in seen_links:
                seen_links.add(tok)
                urls.append(tok)

    # Generate a 64-bit signature for this page using SimHash
    page_val = page_signature(words, k=5)
    if too_similar(page_val):
        return []          

    if len(words) < MIN_WORDS:
        return []
    
    return urls

def make_shingles(words, k = 5):
    # Generate k-word shingles (sliding windows) from token list.
    if len(words) < k:
        return

    for i in range(len(words) - k + 1):
        yield " ".join(words[i:i+k])

def encode_shingle(words):
    # Uses hashlib.blake2b hashfunction to encode shingle into 64 bit int
    h = hashlib.blake2b(words.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, byteorder="big", signed=False)

def bit_difference(a, b):
    # Compute distance between two 64-bit integers.
    return (a ^ b).bit_count()


def page_signature(words, k = 5) -> int:
    # initialize a 64-dimension vote vector

    v = [0] * 64
    for sh in make_shingles(words, k):
        h = encode_shingle(sh)  # 64-bit int
        for i in range(64):
            if (h >> i) & 1:
                #if bit == 1: add vote
                v[i] += 1
            else:
                #if bit == 0: remove vote
                v[i] -= 1
    fp = 0
    for i in range(64):
        if v[i] >= 0:
            # If more positive votes than negative, set bit to 
            fp |= (1 << i)
    
    return fp

def too_similar(fp: int) -> bool:
    # Compare this signatures against all previously seen signatures.
    # Returns True if any are within HAMMING_THRESH.
    for old in seen_signature:
        if bit_difference(fp, old) <= HAMMING_THRESH:
            return True
    seen_signature.append(fp)
    return False


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

def extract_text(html: bytes, url: str) -> Iterable[Tuple[str, str]]:
    # creates a BeautifulSoup object that helps parse html beautifully
    # make sure to run {pip install beautifulsoup4}
    parser = BeautifulSoup(html, "html.parser")
    # we parse through each item in the html

    do_not_parse = {'style', 'title', '[document]', 'script', 'meta', 'head'}

    url, _ = urldefrag(url)

    for item in parser.body.find_all(True):
        # ensures that we DO NOT PARSE through potential style objects or javascript
        if item.name in do_not_parse:
            continue
        # we will only parse text from the parent once bc of recursive=False
        text = "".join(item.find_all(string=True, recursive=False)).strip()
        if (item.name == 'a' and item.get("href")):
            href = item.get("href")
            if href:
                href = href.strip()

                low = href.lower()
                # skip non-crawl schemes
                if low.startswith(("mailto:", "javascript:", "tel:")):
                    href = None

            if href:
                abs_url = urljoin(url, href)     # make absolute
                abs_url, _ = urldefrag(abs_url)       
                yield abs_url, "URL"

        if (text):
            # split text into tokens (split on whitespace)
            tokens = format_alphanum(text)
            # list to store formatted tokens where token is first converted to lowercase then made into a Token object            
            for token in tokens:
                if token:
                    yield token, "word"
    # Parses the html
    # Yields a stream of tokens of either words or URLS with an identifier constructed as Tuple
    # EX: ("hello", "word"), ("www.ics.uci.edu/", "URL")
    
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


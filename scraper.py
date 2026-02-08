import re
from urllib.parse import urlparse, urldefrag, urljoin, parse_qsl, parse_qs, urlencode
from typing import Iterable, Tuple
from bs4 import BeautifulSoup
from utils import get_logger
from collections import defaultdict
from collections import Counter

MAX_HTML_BYTES = 5000000
MIN_WORDS = 50
MAX_URL_LEN = 115
HAMMING_THRESH = 3

# To store the signature for similarity of pages we have already accepted
seen_signature = []
logger = get_logger("SCRAPER")

found_pages = set()
longest_page, longest_length = "", 0
word_freq = defaultdict(int)
STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
    "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
    "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
    "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
    "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
    "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
    "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
    "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
    "that", "that's", "the", "their", "theirs", "them", "themselves", "then",
    "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
    "this", "those", "through", "to", "too", "under", "until", "up", "very", "was",
    "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what",
    "what's", "when", "when's", "where", "where's", "which", "while", "who",
    "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
    "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves"
}

sub_domains = defaultdict(int)


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links]

def extract_next_links(url, resp):
    global longest_page, longest_length
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    found_pages.add(urldefrag(url).url)
    sub_domains[urlparse(url).hostname] += 1

    if resp.status != 200: # TODO: make this less strict?
        return []

    if resp.error: 
        return []

    raw_response = resp.raw_response
    if not raw_response: 
        return []

    html = raw_response.content
    if not html: 
        return []

    if len(html) > MAX_HTML_BYTES: # low value pages to filter before doing token work 
        logger.info(f"ignoring url {url} with page size: {len(resp.raw_response.content)}")
        return []

    urls = []
    words = []

    for tok, kind in extract_text(html, url):
        if kind == "word":
            if tok and tok not in STOP_WORDS:
                word_freq[tok] += 1
                words.append(tok)
        elif kind == "URL":
            norm = normalize_url(tok)
            if norm and norm not in found_pages and is_valid(norm):
                found_pages.add(norm)
                urls.append(norm)

    fp = page_signature(words, k=5)
    if too_similar(fp):
        logger.info(f"near-duplicate (simhash) skipped: {url}")
        return []

    if len(words) == 0: # no information
        return []

    value_score = len(words)/len(html)

    if len(html) > MAX_HTML_BYTES/2 and value_score < 0.05: # low value pages
        logger.info(f"ignoring url {url} with page size: {len(resp.raw_response.content)}, value_score: {value_score}")
        return []

    if len(words) > longest_length: 
        longest_length = len(words)
        longest_page = url

    return urls

def make_shingles(words, k = 5):
    # Generate k-word shingles (sliding windows) from token list.
    if len(words) < k:
        return

    for i in range(len(words) - k + 1):
        yield " ".join(words[i:i+k])

def encode_shingle(words):
    # Encodes using a FNV-1a hash function
    h = 14695981039346656037  # offset basis
    fnv_prime = 1099511628211

    for b in words.encode("utf-8"):
        h ^= b
        h = (h * fnv_prime) & 0xFFFFFFFFFFFFFFFF  # keep 64 bits

    return h

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

regex = re.compile(r"[A-Za-z0-9]+(?:'[A-Za-z0-9]+)?") # Compile globally

def extract_text(html: bytes, url: str) -> Iterable[Tuple[str, str]]:
    # creates a BeautifulSoup object that helps parse html beautifully
    # make sure to run {pip install beautifulsoup4}
    parser = BeautifulSoup(html, "html.parser")
    # we parse through each item in the html

    do_not_parse = {'style', 'title', 'script', 'noscript', 'meta', 'head'}

    url, _ = urldefrag(url)

    body = parser.body
    if body is None:
        return

    for tag in body.find_all(do_not_parse):
        tag.decompose()

    for item in body.find_all("a", href=True):
        if "nofollow" in (item.get("rel") or []):
            continue

        href = item.get("href")
        if href:
            href = href.strip()
            low = href.lower()

            # skip non-crawl schemes
            if low.startswith(("mailto:", "javascript:", "tel:", "#")):
                href = None

        if href:
            try:
                abs_url = urljoin(url, href)
            except:
                continue
            abs_url, _ = urldefrag(abs_url)       
            yield abs_url, "URL"

    for text in body.stripped_strings:
        for token in regex.findall(text.lower()):
            yield token, "word"
            # split text into tokens (split on whitespace)
            # list to store formatted tokens where token is first converted to lowercase then made into a Token object            

    # Parses the html
    # Yields a stream of tokens of either words or URLS with an identifier constructed as Tuple
    # EX: ("hello", "word"), ("www.ics.uci.edu/", "URL")
    
def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        if len(url) > MAX_URL_LEN: # potential crawler trap: URL getting longer
            return False
        parsed = urlparse(url)

        if parsed.scheme not in set(["http", "https"]):
            return False

        allowed = {"ics.uci.edu", "cs.uci.edu", "informatics.uci.edu", "stat.uci.edu"}
        if not any(parsed.hostname == d or parsed.hostname.endswith("." + d) for d in allowed):
            return False

        if "do" in parse_qs(parsed.query): # do=_ query trap
            logger.info(f"detected and skipped do=_ query trap at: {url}")
            return False

        if parsed.netloc == "grape.ics.uci.edu": # handling grape.ics.uci.edu trap
            if "wiki" in parsed.path and "timeline" in parsed.path:
                return False
            queries = parse_qs(parsed.query)
            if "action" in queries: 
                return False
            if "version" in queries: 
                return False

        if (parsed.netloc=="isg.ics.uci.edu" or parsed.netloc=="wics.ics.uci.edu") and "/events/" in parsed.path: # calendar trap
            logger.info(f"detected and skipped calendar trap at: {url}")
            return False

        if (parsed.netloc=="ics.uci.edu"): # calendar trap
            if "/events" in parsed.path:
                if not "/events/list" in parsed.path: 
                    logger.info(f"detected and skipped calendar trap at: {url}")
                    return False

        # Skip photo gallery directories (low textual value, many near-duplicate pages)
        if "/pix/" in parsed.path:
            return False
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpg|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|war)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise

def normalize_url(url): 
    try:
        p = urlparse(url)
    except ValueError:
        return None

    if p.scheme not in ("http", "https"):
        return None

    if p.hostname is None:
        return None

    new_url = f"{p.scheme}://{p.hostname}"
    
    if p.port and p.port not in (80, 443):
        new_url += f":{p.port}"

    new_url += p.path

    if p.query:
        # handling C=_;O=_ trap
        query = p.query.replace(";", "&")
        kept = [
            (k, v) for (k, v) in parse_qsl(query, keep_blank_values=True)
            if k.lower() not in {"c", "o"}
        ]
        new_query = urlencode(kept, doseq=True)
        new_url += f"?{new_query}"

    return new_url

def finish(): # print out statistics
    logger.info(f"unique pages: {len(found_pages)}")
    logger.info(f"longest page: {longest_page}, length: {longest_length}")
    logger.info(f"most common 50: {Counter(word_freq).most_common(50)}")
    logger.info(f"subdomains: ")
    for key, value in sorted(sub_domains.items()): 
        logger.info(f"{key}, {value}")


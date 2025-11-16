"""Debug Indeed RSS feed."""

import requests

url = "https://ca.indeed.com/rss?q=&l=Toronto&limit=10"

response = requests.get(url)
print(f"Status Code: {response.status_code}")
print(f"Content-Type: {response.headers.get('content-type')}")
print(f"\nFirst 500 chars of response:")
print(response.text[:500])

# Databricks notebook source
import requests
from typing import Dict, Any, List

def fetch_from_facebook(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    print(f"Fetching URL: {url} with params: {params}")
        try:
                response = requests.get(url, params=params)
                        response.raise_for_status()
                                data = response.json()
                                        print(f"Data fetched successfully: {data}")
                                                return data
                                                    except requests.exceptions.RequestException as e:
                                                            print(f"Request failed: {e}")
                                                                    return {}
                                                                        except ValueError as e:
                                                                                print(f"Failed to parse response: {e}")
                                                                                        return {}

                                                                                        def fetch_all_posts(access_token: str, group_id: str) -> List[Dict[str, Any]]:
                                                                                            url = f"https://graph.facebook.com/{group_id}/feed"
                                                                                                params = {
                                                                                                        'access_token': access_token,
                                                                                                                'limit': 100
                                                                                                                    }
                                                                                                                        all_posts = []
                                                                                                                            
                                                                                                                                while url:
                                                                                                                                        data = fetch_from_facebook(url, params)
                                                                                                                                                posts = data.get('data', [])
                                                                                                                                                        print(f"Fetched {len(posts)} posts")
                                                                                                                                                                all_posts.extend(posts)
                                                                                                                                                                        url = data.get('paging', {}).get('next')
                                                                                                                                                                                if url:
                                                                                                                                                                                            print(f"Next page URL: {url}")

                                                                                                                                                                                                return all_posts

                                                                                                                                                                                                if __name__ == "__main__":
                                                                                                                                                                                                    ACCESS_TOKEN = 'YOUR_ACCESS_TOKEN'
                                                                                                                                                                                                        GROUP_ID = 'YOUR_GROUP_ID'
                                                                                                                                                                                                            
                                                                                                                                                                                                                posts = fetch_all_posts(ACCESS_TOKEN, GROUP_ID)
                                                                                                                                                                                                                    for post in posts:
                                                                                                                                                                                                                            print(post)

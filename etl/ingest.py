
import requests
import json
import os
import click
from time import sleep

def ingest_messages(group_id, page_size, max_pages) -> int:
    """
    Extracts chat history from the API and saves it as JSON files.
    
    :param group_id: ID of the group to extract messages from
    :param page_size: Number of messages per page
    :param max_pages: Maximum number of pages to fetch
    :return: Number of pages successfully fetched
    """
    headers = {
        "Content-Type": "application/json"
    }

    token = os.environ["GROUPME_TOKEN"]
    
    url = f"https://api.groupme.com/v3/groups/{group_id}/messages?token={token}"
    params = {"limit": min(page_size, 100)}  # Ensure page_size doesn"t exceed 100
    
    pages_fetched = 0
    last_message_id = None
    all_messages = []
    
    def write_to_file():
        if all_messages:
            oldest_timestamp = all_messages[-1]["created_at"]
            filename = f"{group_id}_{oldest_timestamp}_{last_message_id}.json"
            filepath = os.path.join(".local_data", "json", filename)
            with open(filepath, "w") as f:
                json.dump(all_messages, f, indent=2)
            click.echo(f"\nSaved {len(all_messages)} messages to {filename}\n")
            all_messages.clear()

    try:
        with click.progressbar(length=max_pages, label="Fetching messages") as bar:
            while True:
                if last_message_id:
                    params["before_id"] = last_message_id
                
                sleep(2)
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                response_data = response.json()["response"]

                messages = response_data.get("messages", [])
                
                if not messages:
                    click.echo("No messages in the response.")
                    break
                
                all_messages.extend(messages)
                pages_fetched += 1
                last_message_id = messages[-1]["id"]
                
                bar.update(1)
                
                if pages_fetched % 10 == 0 or (max_pages and pages_fetched >= max_pages):
                    write_to_file()
                
                if max_pages and pages_fetched >= max_pages:
                    click.echo(f"Reached maximum number of pages ({max_pages}).")
                    break
    
    except requests.exceptions.RequestException as e:
        click.echo(f"Error occurred while fetching page {pages_fetched + 1}: {e}", err=True)
    
    finally:
        # Write any remaining messages
        write_to_file()
    
    click.echo(f"Successfully fetched {pages_fetched} pages.")
    return pages_fetched

def ingest_groups() -> None:
    headers = {
        "Content-Type": "application/json"
    }

    token = os.environ["GROUPME_TOKEN"]
    
    url = f"https://api.groupme.com/v3/groups?token={token}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    groups = response.json()["response"]
    filepath = os.path.join(".local_data", "json", "groups", "groups.json")
    with open(filepath, "w") as f:
        json.dump(groups, f, indent=2)



@click.command()
@click.argument("target")
@click.option("--group-id", help="group ID, retrieve from groupme API")
@click.option("--page-size", default=20, help="Number of messages per page (default 20, max 100)")
@click.option("--max-pages", default=None, type=int, help="Maximum number of pages to fetch (default: fetch all)")
def main(target, group_id, page_size, max_pages):
    if(target == "messages"):
        ingest_messages(group_id, page_size, max_pages)
    elif(target == "groups"):
        ingest_groups()

if __name__ == "__main__":
    main()

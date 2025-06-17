import os
import json
from openai import OpenAI
from tqdm import tqdm

# Initialize the OpenAI client with API key securely
client = OpenAI(api_key=os.environ.get("ADD_API_KEY"))

# Define the static part of the prompt that will be cached

SYSTEM_PROMPT = system_prompt_template = (
    """Climate Change Stance Analysis Prompt"

    You are tasked with analyzing a pair of text entries, a "Comment" and its "Parent Comment", to determine the stance towards climate change.
    
    Analyze the stance of the "Response Comment" towards climate change by following these steps:
    1. Read and understand the Parent Comment's climate stance
    2. Analyze how the Response Comment relates to it, watching for:
    
    IMMEDIATELY classify as 0 (Against) if the comment:
    
    - dismisses climate activists (especially based on age/gender)
    - Supports known climate deniers or skeptics
    - Thanks or praises those opposing climate science
    - Uses dismissive language about climate concerns
    - Makes fun of climate activists
    - Agrees with climate skepticism
    
    IMMEDIATELY classify as 2 (Support) if the comment:
    - Defends climate activists
    - Criticizes climate deniers
    - Shows frustration with climate inaction
    - Supports those speaking about climate
    - Criticizes those who "have no clue" about climate
    — Expresses concern about climate impacts
    
    Only classify as 1 (Neutral) if the comment:
    - Is completely off-topic
    - Is in a foreign language
    - Contains no stance indicators whatsoever
    
    Answer only with the number, nothing else.
    The following is the content to analyze:
    """
)


def label_comments(data, batch_size=10):
    labeled_data = []

    for i in tqdm(range(0, len(data), batch_size), desc="Processing batches"):
        batch = data[i:i + batch_size]
        batch_messages = []

        for item in batch:
            parent_comment = item.get('ParentCommentText', item.get('VideoID'))
            response_comment = item['CommentText']
            user_content = f"Parent Comment: {parent_comment}\nComment: {response_comment}"

            batch_messages.append({
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_content}
                ],
                "original_item": item
            })

        try:
            responses = [
                client.chat.completions.create(
                    messages=msg["messages"],
                    model="gpt-4o",
                    max_tokens=5,
                    temperature=0.1
                )
                for msg in batch_messages
            ]

            for response, msg in zip(responses, batch_messages):
                item = msg["original_item"].copy()
                try:
                    item['Stance_Label'] = int(response.choices[0].message.content.strip())
                    if hasattr(response, 'usage') and hasattr(response.usage, 'prompt_tokens_details'):
                        item['cached_tokens'] = getattr(response.usage.prompt_tokens_details, 'cached_tokens', 0)
                except (ValueError, AttributeError) as e:
                    print(f"Error processing response: {e}")
                    item['Stance_Label'] = None
                labeled_data.append(item)

        except Exception as e:
            print(f"Batch processing error: {e}")
            for msg in batch_messages:
                item = msg["original_item"].copy()
                item['Stance_Label'] = None
                labeled_data.append(item)

    return labeled_data


def process_file(input_file, output_file):
    with open(input_file, 'r') as file:
        data = [json.loads(line) for line in file]

    labeled_data = label_comments(data)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as file:
        for entry in labeled_data:
            file.write(json.dumps(entry) + '\n')


def main():
    input_dir = 'INSERT_PATH'
    output_dir = 'INSERT_PATH'

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process each file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith('.jsonl'):
            # Create output filename by replacing MAP_Precomments with Label_
            output_filename = filename.replace('MAP_Precomments_', 'Label_')

            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, output_filename)

            print(f"Processing {filename}...")
            process_file(input_path, output_path)
            print(f"Completed processing {filename}")


if __name__ == "__main__":
    main()

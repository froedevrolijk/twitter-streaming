# Twitter Streaming

### Setup environment
- Create a virtual environment and activate it
- Provide credentials in src/config/config.py
- Install requirements: ``` pip install -r requirements.txt ```
- Run tests: ``` python -m pytest tests ```
- Run twitter_streaming.py


### Functionality
The app has the following functionality:
- Connects to the Twitter Streaming API.

- Filters messages that track on "bieber".

- Retrieves the incoming messages for 30 seconds or up to 100 messages, whichever comes first. 

- For each message, the following information is displayed:  
    - The message ID
    - The creation date of the message as epoch value  
    - The text of the message  
    - The author of the message  


- For each author, the following information is displayed:  
    - The user ID  
    - The creation date of the user as epoch value  
    - The name of the user  
    - The screen name of the user  


- The application returns the messages grouped by user (users sorted chronologically, ascending).  
- The messages per user are als sorted chronologically, ascending.  
- The output information is printed to a tab-separated file, with the header containing the column names.  
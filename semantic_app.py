import os
from dotenv import load_dotenv
import pprint

import streamlit as st # Import python packages
from snowflake.snowpark.context import get_active_session

from snowflake.cortex import Complete
from snowflake.core import Root

import pandas as pd
import json

pd.set_option("max_colwidth",None)

from snowflake.snowpark import Session

load_dotenv()

### Default Values
NUM_CHUNKS = 10 # Num-chunks provided as context. Play with this to check how it affects your accuracy
slide_window = 7 # how many last conversations to remember. This is the slide window.

# service parameters
CORTEX_SEARCH_DATABASE = "SEMANTIC_SEARCH_DB"
CORTEX_SEARCH_SCHEMA = "DOCS_SCHEMA"
CORTEX_SEARCH_SERVICE = "SEMANTIC_CORTEX_SEARCH_SERVICE"
######
######

# columns to query in the service
COLUMNS = [
    "doc_text",
    "relative_path",
    "subject",
    "file_url"
]

@st.cache_resource
def get_snowpark_session():
    connection_parameters = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_USER_PASSWORD"],
        "role": "ACCOUNTADMIN",
        "database": "SEMANTIC_SEARCH_DB",
        "warehouse": "SEMANTIC_WH",
        "schema": "DOCS_SCHEMA",
    }
    return Session.builder.configs(connection_parameters).create()

# Use the cached session
session = get_snowpark_session()
root = Root(session)                    

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
   
### Functions
     
def config_options():

    st.sidebar.selectbox('Select your model:',(
                                    'mistral-large2',
                                     ), key="model_name")

    subjects = session.table('education_docs').select('subject').distinct().collect()

    sub_list = ['ALL']
    for sub in subjects:
        sub_list.append(sub.SUBJECT)
            
    st.sidebar.selectbox('Select what products you are looking for', sub_list, key = "subject_value")

    st.sidebar.checkbox('Use your own documents as context?', key="use_docs", value = True)

    st.sidebar.checkbox('Do you want that I remember the chat history?', key="use_chat_history", value = True)

    st.sidebar.checkbox('Debug: Click to see summary generated of previous conversation', key="debug", value = True)
    st.sidebar.button("Start Over", key="clear_conversation", on_click=init_messages)
    st.sidebar.expander("Session State").write(st.session_state)

def init_messages():

    # Initialize chat history
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def get_similar_chunks_search_service(query):

    if st.session_state.subject_value == "ALL":
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    else: 
        filter_obj = {"@eq": {"subject": st.session_state.subject_value} }
        response = svc.search(query, COLUMNS, filter=filter_obj, limit=NUM_CHUNKS)

    st.sidebar.json(response.model_dump_json())
    
    return response.model_dump_json()  

def get_chat_history():
#Get the history from the st.session_stage.messages according to the slide window parameter
    
    chat_history = []
    
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range (start_index , len(st.session_state.messages) -1):
         chat_history.append(st.session_state.messages[i])

    return chat_history

def summarize_question_with_history(chat_history, question):
# To get the right context, use the LLM to first summarize the previous conversation
# This will be used to get embeddings and find similar chunks in the docs for context

    prompt = f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natual language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    
    sumary = Complete(st.session_state.model_name, prompt, session=session)   

    if st.session_state.debug:
        st.sidebar.text("Summary to be used to find similar chunks in the docs:")
        st.sidebar.caption(sumary)

    sumary = sumary.replace("'", "")

    return sumary

def create_prompt (myquestion):

    if st.session_state.use_docs:
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()

            if chat_history != []: #There is chat_history, so not first question
                question_summary = summarize_question_with_history(chat_history, myquestion)
                prompt_context =  get_similar_chunks_search_service(question_summary)
            else:
                prompt_context = get_similar_chunks_search_service(myquestion) #First question when using history
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)
            chat_history = ""
  
        prompt = f"""
            You are an expert chat assistance that extracs information from the CONTEXT provided
            between <context> and </context> tags.
            You offer a chat experience considering the information included in the CHAT HISTORY
            provided between <chat_history> and </chat_history> tags..
            When ansering the question contained between <question> and </question> tags
            be concise and do not hallucinate. 
            If you don´t have the information just say so.
            
            Do not mention the CONTEXT used in your answer.
            Do not mention the CHAT HISTORY used in your asnwer.

            Only anwer the question if you can extract it from the CONTEXT provideed.
            
            <chat_history>
            {chat_history}
            </chat_history>
            <context>          
            {prompt_context}
            </context>
            <question>  
            {myquestion}
            </question>
            Answer: 
            """
        
        json_data = json.loads(prompt_context)

        relative_paths = set(item['relative_path'] for item in json_data['results'])

    else:     
        if st.session_state.use_chat_history:
            chat_history = get_chat_history()

            if chat_history != []: #There is chat_history, so not first question
                question_summary = summarize_question_with_history(chat_history, myquestion)
                prompt_context =  get_similar_chunks_search_service(question_summary)
            else:
                prompt_context = get_similar_chunks_search_service(myquestion) #First question when using history
        else:
            prompt_context = get_similar_chunks_search_service(myquestion)
            chat_history = ""
        prompt = f"""
            You offer a chat experience considering the information included in the CHAT HISTORY
            provided between <chat_history> and </chat_history> tags..
            When ansering the question contained between <question> and </question> tags
            be concise. 
            
            Do not mention the CHAT HISTORY used in your asnwer.

            <chat_history>
            {chat_history}
            </chat_history>
            <question>  
            {myquestion}
            </question>
            Answer: 
            """
        relative_paths = "None"

    return prompt, relative_paths


def answer_question(myquestion):

    prompt, relative_paths =create_prompt (myquestion)

    response = Complete(st.session_state.model_name, prompt, session=session)   

    return response, relative_paths

def main():
    
    st.title(f":speech_balloon: Chat Document Assistant with Snowflake Cortex")
    docs_available = session.sql("ls @docs").collect()
    list_docs = []
    for doc in docs_available:
        list_docs.append(doc["name"])
    st.dataframe(list_docs)

    config_options()
    init_messages()
     
    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Accept user input
    if question := st.chat_input("What do you want to know about your products?"):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": question})
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(question)
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
    
            question = question.replace("'","")
    
            with st.spinner(f"{st.session_state.model_name} thinking..."):
                response, relative_paths = answer_question(question)            
                response = response.replace("'", "")
                message_placeholder.markdown(response)

                if relative_paths != "None":
                    with st.sidebar.expander("Related Documents"):
                        for path in relative_paths:
                            cmd2 = f"""
                            SELECT DISTINCT FILE_URL 
                            FROM table('education_docs') 
                            WHERE relative_path = '{path}' 
                            """
                            df_url_link = session.sql(cmd2).to_pandas()
                            url_link = df_url_link._get_value(0,'FILE_URL')
                            display_url = f"Doc: [{path}]({url_link})"
                            
                            st.sidebar.markdown(display_url)
                            

        
        st.session_state.messages.append({"role": "assistant", "content": response})


if __name__ == "__main__":
    main()
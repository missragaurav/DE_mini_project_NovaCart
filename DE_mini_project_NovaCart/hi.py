import os
from Bio import Entrez
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain_chroma import Chroma
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
from langchain_classic.chains import create_retrieval_chain
from langchain_classic.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate

# --- CONFIGURATION ---
ENTREZ_EMAIL = ""
NCBI_API_KEY = ""
GEMINI_API_KEY = ""

os.environ["GOOGLE_API_KEY"] = GEMINI_API_KEY

# --- STEP 1: PUBMED SEARCH & FETCH ---
def fetch_pubmed_data(user_query, max_results=3):
    Entrez.email = ENTREZ_EMAIL
    Entrez.api_key = NCBI_API_KEY
    
    print(f" Searching PubMed for: {user_query}...")
    try:
        # Search for IDs
        search_handle = Entrez.esearch(
            db="pubmed",
            term=user_query,
            retmax=max_results,
            sort="relevance"
        )
        search_results = Entrez.read(search_handle)
        search_handle.close()
        
        ids = search_results["IdList"]
        if not ids:
            return []

        # Fetch details
        fetch_handle = Entrez.efetch(
            db="pubmed",
            id=",".join(ids),
            rettype="abstract",
            retmode="xml"
        )
        articles = Entrez.read(fetch_handle)
        fetch_handle.close()

        docs = []
        for article in articles['PubmedArticle']:
            title = article['MedlineCitation']['Article']['ArticleTitle']
            pmid = article['MedlineCitation']['PMID']
            url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
            
            try:
                abstract_list = article['MedlineCitation']['Article']['Abstract']['AbstractText']
                abstract = " ".join([str(text) for text in abstract_list])
            except (KeyError, TypeError):
                abstract = "No abstract available."

            docs.append(Document(
                page_content=abstract,
                metadata={"title": title, "source": url}
            ))

        return docs

    except Exception as e:
        print(f" PubMed API Error: {e}")
        return []

# --- STEP 2: RAG PIPELINE ---
def run_medical_rag():
    print("\n--- Medical Research Assistant (PubMed + Gemini) ---")
    user_input = input("Enter your research question: ")

    # 1. Fetch Articles
    raw_documents = fetch_pubmed_data(user_input)
    if not raw_documents:
        print("No articles found for that topic.")
        return

    # 2. Split into Chunks
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=700,
        chunk_overlap=100
    )
    splits = text_splitter.split_documents(raw_documents)

    # 3. Embed and Store in ChromaDB
    print("Indexing documents...")
    embeddings = GoogleGenerativeAIEmbeddings(
        model="models/gemini-embedding-001"
    )

    vectorstore = Chroma.from_documents(
        documents=splits,
        embedding=embeddings
    )

    # 4. Define Structured Prompt
    system_prompt = (
        "You are a professional medical research assistant. Use the provided PubMed abstracts "
        "to answer the question accurately. If the information is not in the context, "
        "state that you cannot find the answer in the retrieved papers.\n\n"
        "Return the output in this EXACT format:\n"
        "### 1. Summary\n(A 2-sentence high-level overview)\n\n"
        "### 2. Key Findings\n* (Point 1)\n* (Point 2)\n\n"
        "### 3. Citations\n* [Title] - [URL]\n\n"
        "Context: {context}"
    )

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{input}"),
    ])

    # 5. Build and Run Chain
    llm = ChatGoogleGenerativeAI(
        model="models/gemini-2.5-flash",
        temperature=0.1
    )

    question_answer_chain = create_stuff_documents_chain(llm, prompt)
    rag_chain = create_retrieval_chain(
        vectorstore.as_retriever(),
        question_answer_chain
    )

    print("Analyzing research results...")
    response = rag_chain.invoke({"input": user_input})

    print("\n" + "="*60)
    print(response["answer"])

   
    print("\n### Sources Used:")
    retrieved_docs = vectorstore.similarity_search(user_input, k=3)

    seen = set()
    for doc in retrieved_docs:
        title = doc.metadata.get("title", "No title")
        source = doc.metadata.get("source", "No link")

        if source not in seen:
            print(f"- {title}")
            print(f"  {source}")
            seen.add(source)

    print("="*60)


if __name__ == "__main__":
    run_medical_rag()
import os
import nest_asyncio
from llama_index.core import PropertyGraphIndex, Settings
from llama_index.graph_stores.neo4j import Neo4jPropertyGraphStore
from llama_index.llms.ollama import Ollama
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core import PromptTemplate
import time

# Async hatasını önle
nest_asyncio.apply()
CLOUDFLARE_TUNNEL_URL = "https://western-differently-salary-chem.trycloudflare.com/" 
OLLAMA_MODEL_ID = "gemma3:27b"
# ---------------------------------------------------------
# 1. AYARLAR
# ---------------------------------------------------------
JSON_DIR = "./database"       # JSON dosyalarının olduğu klasör
STATE_FILE = "file_state.json" # Hangi dosyanın işlendiğini tutan hafıza dosyası


# ---------------------------------------------------------
# 1. AYARLAR (Database koduyla BİREBİR AYNI olmalı)
# ---------------------------------------------------------
print("⚙️  Ayarlar yükleniyor...")

# LLM: Llama 3.1 8b
Settings.llm = Ollama(
    model=OLLAMA_MODEL_ID, 
    base_url=CLOUDFLARE_TUNNEL_URL, 
    request_timeout=3000.0,
    temperature=0.1
)

# Embedding: Database'de ne kullandıysan AYNISI olmalı
Settings.embed_model = HuggingFaceEmbedding(
    model_name="paraphrase-multilingual-mpnet-base-v2"
)

# ---------------------------------------------------------
# 2. NEO4J BAĞLANTISI
# ---------------------------------------------------------
graph_store = Neo4jPropertyGraphStore(
    username="neo4j",
    password="abcd1234",  # Şifreni buraya yaz
    url="bolt://localhost:7687",
)

# ---------------------------------------------------------
# 3. GRAPH'I YÜKLE (INDEX LOADING)
# ---------------------------------------------------------
print("🔌 Veritabanına bağlanılıyor...")

# "from_documents" YERİNE "from_existing" kullanıyoruz.
# Bu, veriyi yeniden yazmaz, sadece var olanı okur.
index = PropertyGraphIndex.from_existing(
    property_graph_store=graph_store,
    embed_model=Settings.embed_model,
    llm=Settings.llm
)

print("✅ Bağlantı başarılı! Sohbet başlıyor...\n")

# ---------------------------------------------------------
# 4. SORGULAMA MOTORU
# ---------------------------------------------------------
# include_text=True: Hem graph ilişkilerine bak hem de orijinal metne bak (Hybrid Search)
query_engine = index.as_query_engine(
    include_text=True, 
    similarity_top_k=3, # En benzer 3 metni getir
)

def get_answer(question):

    try:
        start_time=time.time()
        response = query_engine.query(question)
        stop_time=time.time()
        elapsed_time=stop_time-start_time

        print(f"\n⭐️ CEVAP:\n{response}")
        print(f"⏱️  Cevap Süresi: {elapsed_time:.2f} saniye")
        print("\n📄 Kaynaklar:")
        for node in response.source_nodes:
            print(f"- {node.text[:100]}...")
    except Exception as e:
        print(f"❌ Hata: {e}")

# ---------------------------------------------------------
# 5. SOHBET DÖNGÜSÜ
# ---------------------------------------------------------
if __name__ == "__main__":
    get_answer( 
    question="Normanlar ile Vikingler arasında nasıl bir ilişki vardır?")

    get_answer( 
    question="Normanların dini inancı ve dili hakkında bilgi ver.")

    get_answer( 
    question="Normanların Frenklerle bir etkileşimi olmuş mudur?")
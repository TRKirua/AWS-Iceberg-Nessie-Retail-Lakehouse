# Cellules à ajouter pour compléter la partie Branching
# Ces cellules doivent être insérées après la cellule 39

BRANCHING_CELLS = [
    {
        "id": 40,
        "type": "code",
        "source": '''# Vérifier que dev a les mêmes données que main (pour l'instant)
print("=== ÉTAT DE 'dev' (identique à main pour l'instant) ===")

dev_bronze = spark.sql("SELECT COUNT(*) FROM nessie.bronze.sales").first()[0]
dev_silver = spark.sql("SELECT COUNT(*) FROM nessie.silver.sales").first()[0]
dev_gold = spark.sql("SELECT COUNT(*) FROM nessie.gold.sales_by_category_region").first()[0]

print(f"Bronze: {dev_bronze:,} enregistrements")
print(f"Silver: {dev_silver:,} enregistrements")
print(f"Gold: {dev_gold:,} agrégats")

print("\\n🔍 Observation: 'dev' est une copie de 'main' au moment de sa création")'''
    },
    {
        "id": 41,
        "type": "markdown",
        "source": '''### Modification sur la branche dev

Maintenant on va faire une **expérimentation** sur dev : ajouter un nouveau batch de données et créer une nouvelle table.
**Important** : Ces modifications n'affectent PAS la branche main !'''
    },
    {
        "id": 42,
        "type": "code",
        "source": '''# EXPÉRIMENTATION sur dev: Ajouter un batch "expérimental"
print("=== EXPÉRIMENTATION SUR 'dev' ===")
print("Ajout d'un batch expérimental de données...")

# Simuler un batch expérimental (on réutilise le batch 3 comme données expérimentales)
batch_exp_path = f"s3a://{AWS_BUCKET}/raw/batches/sales_batch_003.csv"

batch_exp_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .csv(batch_exp_path)
)

batch_exp_bronze = (
    batch_exp_raw
    .withColumn("ingestion_date", current_date())
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", lit("experimental_batch.csv"))
    .withColumn("source_system", lit("experimental_system"))
    .withColumn("batch_id", lit("experimental"))
)

batch_exp_bronze.writeTo("nessie.bronze.sales").append()

dev_bronze_after = spark.sql("SELECT COUNT(*) FROM nessie.bronze.sales").first()[0]
print(f"✅ Batch expérimental ajouté sur 'dev'")
print(f"Bronze sur dev: {dev_bronze_after:,} (+{dev_bronze_after - dev_bronze:,})")'''
    },
    {
        "id": 43,
        "type": "code",
        "source": '''# Transformer le batch expérimental vers Silver
print("=== TRANSFORMATION DU BATCH EXPÉRIMENTAL VERS SILVER ===")

batch_exp_silver = (
    batch_exp_bronze
    .select(
        col("Order ID").alias("order_id"),
        col("Order Date").alias("order_date"),
        col("Ship Date").alias("ship_date"),
        col("Ship Mode").alias("ship_mode"),
        col("Customer ID").alias("customer_id"),
        col("Customer Name").alias("customer_name"),
        col("Segment").alias("segment"),
        col("Country").alias("country"),
        col("City").alias("city"),
        col("State").alias("state"),
        col("Postal Code").alias("postal_code"),
        col("Region").alias("region"),
        col("Product ID").alias("product_id"),
        col("Category").alias("category"),
        col("Sub-Category").alias("sub_category"),
        col("Product Name").alias("product_name"),
        col("Sales").cast("double").alias("sales"),
        col("Quantity").cast("int").alias("quantity"),
        col("Discount").cast("double").alias("discount"),
        col("Profit").cast("double").alias("profit"),
        col("ingestion_date"),
        col("ingestion_ts"),
        col("batch_id")
    )
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
    .filter(col("sales").isNotNull())
    .dropDuplicates(["order_id", "product_id"])
)

batch_exp_silver.writeTo("nessie.silver.sales").append()

dev_silver_after = spark.sql("SELECT COUNT(*) FROM nessie.silver.sales").first()[0]
print(f"✅ Silver mis à jour sur 'dev'")
print(f"Silver sur dev: {dev_silver_after:,} (+{dev_silver_after - dev_silver:,})")'''
    },
    {
        "id": 44,
        "type": "code",
        "source": '''# EXPÉRIMENTATION sur dev: Créer une nouvelle table expérimentale
print("=== CRÉATION D'UNE TABLE EXPÉRIMENTALE SUR 'dev' ===")

# Créer une table expérimentale de stats par ville
city_stats = (
    spark.table("nessie.silver.sales")
    .groupBy("city", "region")
    .agg(
        spark_round(spark_sum("sales"), 2).alias("total_sales"),
        spark_round(spark_sum("profit"), 2).alias("total_profit"),
        count("order_id").alias("order_count")
    )
    .filter(col("total_sales") > 50000)
)

city_stats.writeTo("nessie.gold.city_stats_experimental").using("iceberg").create()

print("✅ Table expérimentale 'city_stats_experimental' créée sur 'dev'")
print(f"Nombre de villes: {city_stats.count():,}")

print("\\nTop 5 villes:")
spark.sql("""
    SELECT city, region, total_sales, order_count
    FROM nessie.gold.city_stats_experimental
    ORDER BY total_sales DESC
    LIMIT 5
""").show(truncate=False)'''
    },
    {
        "id": 45,
        "type": "code",
        "source": '''# COMPARAISON: Revenir sur main et vérifier qu'il n'a PAS changé
print("=== COMPARAISON: RETOUR SUR 'main' ===")
print("Changement de branche...")
spark.conf.set("spark.sql.catalog.nessie.ref", "main")

print("✅ De retour sur 'main'")
print(f"Branche actuelle: {spark.conf.get('spark.sql.catalog.nessie.ref')}")'''
    },
    {
        "id": 46,
        "type": "code",
        "source": '''# Vérifier que main n'a PAS changé
print("=== ÉTAT DE 'main' (TOUJOURS IDENTIQUE) ===")

main_bronze_check = spark.sql("SELECT COUNT(*) FROM nessie.bronze.sales").first()[0]
main_silver_check = spark.sql("SELECT COUNT(*) FROM nessie.silver.sales").first()[0]
main_gold_check = spark.sql("SELECT COUNT(*) FROM nessie.gold.sales_by_category_region").first()[0]

print(f"Bronze: {main_bronze_check:,} enregistrements")
print(f"Silver: {main_silver_check:,} enregistrements")
print(f"Gold: {main_gold_check:,} agrégats")

print("\\n🔍 CLÉ: Les chiffres sont identiques à avant!")
print(f"   Bronze avant: {main_bronze:,} | Bronze maintenant: {main_bronze_check:,}")'''
    },
    {
        "id": 47,
        "type": "code",
        "source": '''# Essayer d'accéder à la table expérimentale (doit échouer sur main)
print("=== TEST D'ACCÈS À LA TABLE EXPÉRIMENTALE ===")
print("La table 'city_stats_experimental' existe-t-elle sur main?")

try:
    spark.sql("SELECT * FROM nessie.gold.city_stats_experimental LIMIT 1").count()
    print("❌ Erreur: La table ne devrait pas exister sur main!")
except Exception as e:
    print("✅ La table n'existe PAS sur main (c'est normal)")
    print("   Elle existe uniquement sur la branche 'dev'")'''
    },
    {
        "id": 48,
        "type": "code",
        "source": '''# Comparaison directe entre main et dev
print("=== COMPARAISON DIRECTE: main vs dev ===")

print("Données sur main (branche actuelle):")
main_count_final = spark.sql("SELECT COUNT(*) FROM nessie.bronze.sales").first()[0]
print(f"  Bronze: {main_count_final:,} enregistrements")

# Utiliser la syntaxe de branche Nessie pour accéder à dev directement
print("\\nDonnées sur dev (accès direct via Nessie):")
dev_count_check = spark.sql("SELECT COUNT(*) FROM nessie.branch_dev.bronze.sales").first()[0]
print(f"  Bronze: {dev_count_check:,} enregistrements")

print(f"\\nDifférence: {dev_count_check - main_count_final:,} enregistrements de plus sur dev")
print("🔍 Ces enregistrements sont le batch expérimental!")'''
    },
    {
        "id": 49,
        "type": "code",
        "source": '''# MERGE: Intégrer dev dans main
print("=== MERGE: dev → main ===")
print("Merge de la branche 'dev' dans 'main'...")

# S'assurer qu'on est sur main pour le merge
spark.conf.set("spark.sql.catalog.nessie.ref", "main")

spark.sql("MERGE BRANCH dev INTO main IN nessie")

print("✅ Merge terminé!")'''
    },
    {
        "id": 50,
        "type": "code",
        "source": '''# Vérifier que main a maintenant les changements
print("=== ÉTAT DE 'main' APRÈS MERGE ===")

main_after_merge = spark.sql("SELECT COUNT(*) FROM nessie.bronze.sales").first()[0]
main_silver_after = spark.sql("SELECT COUNT(*) FROM nessie.silver.sales").first()[0]

print(f"Bronze: {main_after_merge:,} enregistrements (avant: {main_count_final:,})")
print(f"Silver: {main_silver_after:,} enregistrements")
print(f"Différence: +{main_after_merge - main_count_final:,} enregistrements")'''
    },
    {
        "id": 51,
        "type": "code",
        "source": '''# Vérifier que la table expérimentale est maintenant sur main
print("=== TABLE EXPÉRIMENTALE SUR main ===")
print("La table 'city_stats_experimental' existe-t-elle sur main?")

try:
    table_exists = spark.sql("""
        SELECT COUNT(*)
        FROM nessie.gold.city_stats_experimental
    """).first()[0]
    print(f"✅ OUI! La table existe avec {table_exists:,} villes")

    print("\\nContenu de la table expérimentale:")
    spark.sql("""
        SELECT city, region, total_sales, order_count
        FROM nessie.gold.city_stats_experimental
        ORDER BY total_sales DESC
        LIMIT 5
    """).show(truncate=False)
except Exception as e:
    print("❌ La table n'existe pas")'''
    },
    {
        "id": 52,
        "type": "code",
        "source": '''# Optionnel: Supprimer la branche dev après merge réussi
print("=== SUPPRESSION DE LA BRANCHE 'dev' ===")
print("Suppression de la branche 'dev' (le merge est réussi)...")

spark.conf.set("spark.sql.catalog.nessie.ref", "main")

spark.sql("DROP BRANCH IF EXISTS dev IN nessie")

print("✅ Branche 'dev' supprimée!")

print("\\nBranches restantes:")
spark.sql("LIST REFERENCES IN nessie").show(truncate=False)'''
    },
    {
        "id": 53,
        "type": "markdown",
        "source": '''---
## RÉCAPITULATIF'''
    },
    {
        "id": 54,
        "type": "code",
        "source": '''# Récapitulatif complet de la démo
print("\\n" + "="*60)
print("🎯 RÉCAPITULATIF COMPLET DE LA DÉMO")
print("="*60)

print("""
✅ Pipeline multi-batch avec incrémental:
   • Batch 1 (matin): 3,364 enregistrements
   • Batch 2 (midi):  +3,501 enregistrements
   • Batch 3 (après-midi): +3,500 enregistrements

✅ Time Travel - Voyage dans le temps:
   • Requêter les données à différents moments
   • Comparer l'évolution des agrégats
   • Snapshots conservés pour chaque opération

✅ Rollback - Retour à un état précédent:
   • Revenir à l'état après Batch 2
   • Supprimer les données du Batch 3
   • L'historique reste consultable

✅ Branching Nessie - Git pour les données:
   • Créer une branche 'dev' pour l'expérimentation
   • Faire des modifications isolées sans toucher à 'main'
   • Comparer les données entre branches
   • Merger 'dev' dans 'main' quand c'est validé
   • Supprimer la branche après merge

🚀 Avantages Iceberg + Nessie:
   • ACID transactions: pas de corruption
   • Schema evolution: changer le schéma sans casser les données
   • Time Travel: auditer et comparer
   • Rollback: récupérer après erreur
   • Branching: Git-like pour les données!
   • Merge integration: promotion dev → prod
""")

print("\\n" + "="*60)
print("🏆 DÉMO TERMINÉE")
print("="*60)'''
    }
]

# Afficher le nombre de cellules à ajouter
print(f"Nombre de cellules à ajouter: {len(BRANCHING_CELLS)}")
for cell in BRANCHING_CELLS:
    print(f"  - Cell {cell['id']}: {cell['type']}")

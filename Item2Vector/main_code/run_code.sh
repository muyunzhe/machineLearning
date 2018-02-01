python item2vec.py \
  --load_file=../flight/data/temp.txt \
  --save_path=./flight/result \
  --similarity_file=similarity_temp.txt \
  --embed_file=temp_embeddings.csv \
  --hql_file=create_table_temp.hql \
  --hql_table_name=temp.item_feature_temp \
  --embedding_size=32 \
  --vocabulary_size=180 \
  --num_steps=200 \
  --suffix='h_'


  --load_file=../flight/data/temp.txt
  --save_path=./flight/result
  --similarity_file=similarity_temp.txt
  --embed_file=temp_embeddings.csv
  --hql_file=create_table_temp.hql
  --hql_table_name=temp.item_feature_temp
  --embedding_size=32
  --vocabulary_size=180
  --num_steps=200
  --suffix='h_'
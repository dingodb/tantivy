// # Regex query example
// This example shows how to use regex in qurry string and how to use RegexQuery

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::QueryParser;
use tantivy::query::RegexQuery;
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::schema::STORED;
use tantivy::schema::TEXT;
use tantivy::{doc, Document, Index, IndexWriter, TantivyDocument};

fn test_regex_query() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let title_field = schema_builder.add_text_field("title", TEXT | STORED);
    // let comment_field = schema_builder.add_text_field("comment", STRING | STORED);

    let comment_field = schema_builder.add_text_field(
        "comment",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    {
        let mut index_writer: IndexWriter = index.writer(15_000_000)?;
        index_writer.add_document(doc!(
            title_field => "The Name of the Wind",
            comment_field => "The Name of the Wind",

        ))?;
        index_writer.add_document(doc!(
            title_field => "The Diary of Muadib",
            comment_field => "The Diary of Muadib",
        ))?;
        index_writer.add_document(doc!(
            title_field => "A Dairy Cow",
            comment_field => "A Dairy Cow",
        ))?;
        index_writer.add_document(doc!(
            title_field => "The Diary of a Young Girl",
            comment_field => "The Diary of a Young Girl",
        ))?;
        index_writer.commit()?;
    }

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let regex_str = r"diar.*";

    {
        let query = RegexQuery::from_pattern(&regex_str, title_field)?;
        let count = searcher.search(&query, &Count)?;
        println!("title regex search count={}", count);

        let docs = searcher.search(&query, &DocSetCollector)?;
        for doc_address in docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("{}", retrieved_doc.to_json(&schema));
        }
    }

    {
        let query = RegexQuery::from_pattern(&regex_str, comment_field)?;
        let count = searcher.search(&query, &Count)?;
        println!("comment regex search count={}", count);

        let docs = searcher.search(&query, &DocSetCollector)?;
        for doc_address in docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("{}", retrieved_doc.to_json(&schema));
        }
    }

    {
        // test regex query parser
        let query_parser = QueryParser::for_index(&index, vec![title_field]);
        let b64 = BASE64.encode(b"diar.*");
        println!("b64={}", b64);
        let query = query_parser.parse_query(&format!("title:RE [{}]", b64))?;
        let count = searcher.search(&query, &Count)?;
        println!("Regex query parse search count={}", count);
        let docs = searcher.search(&query, &DocSetCollector)?;
        for doc_address in docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("{}", retrieved_doc.to_json(&schema));
        }
    }

    Ok(())
}

fn test_regex_lenth_query() -> tantivy::Result<()> {
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_in_ram(schema.clone());

    {
        let mut index_writer: IndexWriter = index.writer(15_000_000)?;
        index_writer.add_document(doc!(title => "a", body => "the"))?;
        index_writer.add_document(doc!(title => "bb", body => "an old"))?;
        index_writer.add_document(doc!(title => "ccc", body=> "send this message to alice"))?;
        index_writer
            .add_document(doc!(title => "dddd", body=> "a lady was riding and old bike"))?;
        index_writer.add_document(doc!(title => "eeeee", body=> "Yes, my lady."))?;
        index_writer.commit()?;
    }

    let reader = index.reader()?;
    let searcher = reader.searcher();

    {
        // query title length is less 3
        let query = RegexQuery::from_pattern("(.{0,2})", title)?;
        let count = searcher.search(&query, &Count)?;
        println!("Regex query parse length search count={}", count);

        let docs = searcher.search(&query, &DocSetCollector)?;
        for doc_address in docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("{}", retrieved_doc.to_json(&schema));
        }
    }

    {
        // test regex query parser
        let query_parser = QueryParser::for_index(&index, vec![title]);
        let b64 = BASE64.encode(b"(.{0,2})");
        let query = query_parser.parse_query(&format!("title:RE [{}]", b64))?;
        let count = searcher.search(&query, &Count)?;
        println!("Regex query string length search count={}", count);
        let docs = searcher.search(&query, &DocSetCollector)?;
        for doc_address in docs {
            let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
            println!("{}", retrieved_doc.to_json(&schema));
        }
    }

    Ok(())
}

fn main() -> tantivy::Result<()> {
    test_regex_query();

    test_regex_lenth_query();

    Ok(())
}

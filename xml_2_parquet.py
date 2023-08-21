"""
Process EPMC open access dumps to parquet
"""

import click
import polars as pl
from lxml import etree
from tqdm.auto import tqdm

from process_xml import get_zipped_file_content, cleanup_input_xml, parse_PMC_XML_core, get_stats, get_body_structure


def split_into_articles(xml_content):
    """
    Split the xml content into individual articles
    """
    root = etree.fromstring(xml_content)
    articles = root.findall(".//article")
    return articles


@click.command()
@click.argument("input", type=click.Path(exists=True))
@click.argument("output", type=click.Path())
def main(input, output):
    """
    Process EPMC open access dumps to parquet
    """
    xml_content = get_zipped_file_content(input)
    articles = split_into_articles(xml_content)

    dump_dict = {"pmcid": [], "pmid": [], "authors": [], "title": [], "publication_date":[], "keywords":[], "abstract":[], "main_text":[]}

    for root in tqdm(articles):
        article_xml = etree.tostring(root)

        dict_doc = parse_PMC_XML_core(article_xml,root,input)


        body_text = []
        for section in dict_doc['body_sections']:
            if len(section['contents']) == 0:
                continue
            section_paragraphs = []
            for a in section['contents']:
                if 'text' not in a:
                    continue
                section_paragraphs.append(a['text'])
            section_text = " ".join(section_paragraphs)
            body_text.append(section_text)
    
        main_text = "\n\n".join(body_text)
        
        dump_dict['pmcid'].append(dict_doc['pmcid'])
        dump_dict['pmid'].append(dict_doc['pmid'])
        dump_dict['authors'].append([a['name'] for a in dict_doc['authors']])
        dump_dict['title'].append(dict_doc['title'])
        dump_dict['publication_date'].append(dict_doc['publication_date'])
        dump_dict['keywords'].append(dict_doc['keywords'])
        dump_dict['abstract'].append(dict_doc['abstract'])
        dump_dict['main_text'].append(main_text)
        
    dataframe = pl.DataFrame(dump_dict)
    print(dataframe)
    dataframe.to_parquet(output)



if __name__ == "__main__":
    main()
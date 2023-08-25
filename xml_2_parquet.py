"""
Process EPMC open access dumps to parquet
"""

import click
import polars as pl
from lxml import etree
from tqdm.auto import tqdm
from pathlib import Path

from process_xml import get_zipped_file_content, handle_supplementary_material_elements, parse_PMC_XML_fast, parse_PMC_XML_core, remove_alternative_title_if_redundant, get_body_structure


def split_into_articles(xml_content):
    """
    Split the xml content into individual articles
    """
    root = etree.fromstring(xml_content)
    for xs in root.xpath('//xref/sup'): xs.getparent().remove(xs)
    for sx in root.xpath('//sup/xref'): sx.getparent().remove(sx)
    etree.strip_tags(root,'sup')

    etree.strip_tags(root,'italic')
    etree.strip_tags(root,'bold')
    etree.strip_tags(root,'sub')
    etree.strip_tags(root,'ext-link')
    etree.strip_tags(root,'floats-wrap')
    etree.strip_tags(root,'floats-group')

    etree.strip_elements(root, 'inline-formula','disp-formula', with_tail=False)
    handle_supplementary_material_elements(root)



    articles = root.findall(".//article")
    return articles


@click.command()
@click.argument("input_path", type=click.Path(exists=True))
@click.argument("output", type=click.Path())
@click.option("--file_index", type=int, default=0)
@click.option("--fast/--slow", default=False)
def main(input_path, output, file_index, fast):
    """
    Process EPMC open access dumps to parquet
    """
    all_dumps = sorted(Path(input_path).glob("*.xml.gz"))
    print(all_dumps)
    

    input_xml = all_dumps[file_index]

    output_fname = input_xml.name.removesuffix("".join(input_xml.suffixes)) + ".parquet"
    output_path = Path(output) / output_fname
    print(output_path)
    if output_path.exists():
        print(f"Output file {output_path} already exists. Skipping.")
        return
    
    xml_content = get_zipped_file_content(input_xml)
    # Preprocessing tasks: simplify / clean up of the original xml
	# To be kept here before any parsing aimed at retrieving data

    articles = split_into_articles(xml_content)
    if fast:
        process_function = parse_PMC_XML_fast
    else:
        process_function = parse_PMC_XML_core

    dump_dict = {"pmcid": [], "pmid": [], "authors": [], "title": [], "publication_date":[], "keywords":[], "abstract":[], "main_text":[]}

    for root in tqdm(articles):
        article_xml = etree.tostring(root)

        dict_doc = process_function(article_xml,root,input)


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
    print(output_path)
    dataframe.write_parquet(output_path)



if __name__ == "__main__":
    main()
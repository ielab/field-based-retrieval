import subprocess
import multiprocessing

path_terrier = '/Volumes/ext3/indeces/terrier-4.2/bin/trec_terrier.sh'
path_index = '/Volumes/ext3/indeces/terrier-4.2/var/index/clueweb12b/'
path_query = '/volumes/ext/data/clef2016/queries2016.xml'
prefix_run = '/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/terrier/clef2016/ter_tuneB_clef2016_a'
finishedFile = 'finished_clef2016_20180531.txt'


completed_set = []
with open(finishedFile) as fr:
    for line in fr:
        temp = line.replace("/volumes/ext/jimmy/experiments/ipm_fielded_retrieval/data/terrier/clef2016/", "").strip()
        temp = temp.replace("ter_tuneB_clef2016_a", "")
        temp = temp.replace(".run", "")
        temp = temp.replace("bt", "")
        temp = temp.replace("bb", "")
        alpha, b_title, b_body = temp.split("_")

        completed_set.append((int(alpha), float(b_title), float(b_body)))
print("Total Completed: {}".format(len(completed_set)))


def ter_search(query_param):
    subprocess.call(path_terrier + query_param, shell=True)
    print('*********** finished ***********')
    return True

# build list of query params for all alpha values
queryParams = []
for b_title in range(0, 101, 20):
    for b_body in range(0, 101, 20):
        for alpha in range(0, 11, 2):
            # only create jobs for parameter combos not in completed_set
            if (alpha, float(b_title)/100, float(b_body)/100) not in completed_set:
                queryParam = ' -r ' \
                            '-Dterrier.index.path={index} ' \
                            '-Dtrec.topics={queryfile} ' \
                            '-DTrecQueryTags.doctag=query ' \
                            '-DTrecQueryTags.process=query,id,title ' \
                            '-DTrecQueryTags.idtag=id ' \
                            '-DTrecQueryTags.casesensitive=false ' \
                            '-Dtrec.model=BM25F ' \
                            '-Dtrec.results.file={prefix_run}{file_id}_bt{b_title}_bb{b_body}.run ' \
                            '-Dc.0={b_title} -Dc.1={b_body} ' \
                            '-Dw.0={w_title} -Dw.1={w_body}'.\
                        format(index=path_index, queryfile=path_query, prefix_run=prefix_run, file_id=alpha,
                            b_title=float(b_title) / 100, b_body=float(b_body) / 100,
                            w_title=alpha, w_body=10-alpha)
                queryParams.append(queryParam)

# call terrier multi - processing
p = multiprocessing.Pool(processes=5)
res = p.map(ter_search, queryParams)



import numpy as np

from mrjob.job import MRJob
from itertools import combinations, permutations
from collections import defaultdict

from scipy.stats.stats import pearsonr


class RestaurantSimilarities(MRJob):

    def steps(self):
        "the steps in the map-reduce process"
        thesteps = [
            self.mr(mapper=self.line_mapper, reducer=self.users_items_collector),
            self.mr(mapper=self.pair_items_mapper, reducer=self.calc_sim_collector)
        ]
        return thesteps

    def line_mapper(self,_,line):
        "this is the complete implementation"
        user_id,business_id,stars,business_avg,user_avg=line.split(',')
        yield user_id, (business_id,stars,business_avg,user_avg)


    def users_items_collector(self, user_id, values):
        user_items = []
        for value in values:
            user_items.append(value)
        #print user_id, user_items
        yield user_id, user_items
        pass


    def pair_items_mapper(self, user_id, values):
        """
        ignoring the user_id key, take all combinations of business pairs
        and yield as key the pair id, and as value the pair rating information
        """
        businesses = defaultdict(list)
        for si in values:
            businesses.update({si[0]:si[1:]})

        output = list(combinations(businesses.keys(), 2))
        sorted_output = map(sorted, output)
        sorted_lst_of_tuples = [tuple(l) for l in sorted_output]

        for item in sorted_lst_of_tuples:
            yield item, (businesses.get(item[0]), businesses.get(item[1]))

    def calc_sim_collector(self, key, values):
        """
        Pick up the information from the previous yield as shown. Compute
        the pearson correlation and yield the final information as in the
        last line here.
        """
        (rest1, rest2), common_ratings = key, values
        common_ratings_list = list(common_ratings)
        n_common = len(common_ratings_list)
       
        if n_common==0:
            rho=0.
        else:
            diff1 = []
            diff2 = []
            for i in common_ratings_list:
                diff1_item=float(i[0][0])-float(i[0][2])
                diff1.append(diff1_item)
                diff2_item=float(i[1][0])-float(i[1][2])
                diff2.append(diff2_item)
            rho=pearsonr(diff1, diff2)[0]
            if np.isnan(rho)==True:
                rho = 0.
       
        yield (rest1, rest2), (rho, n_common)


#Below MUST be there for things to work
if __name__ == '__main__':
    RestaurantSimilarities.run()
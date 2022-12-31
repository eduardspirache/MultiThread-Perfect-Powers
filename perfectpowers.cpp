#include <bits/stdc++.h>

using namespace std;

typedef struct {
	int id;
	int max_pow;
	vector<vector<vector<int>>>* pow_list;
	queue<string>* file_list;
	pthread_mutex_t* mutex;
	pthread_barrier_t* barrier;
} mapper_args;

typedef struct {
	int id;
	int mapper_cnt;
	vector<vector<vector<int>>>* pow_list;
	pthread_mutex_t* mutex;
	pthread_barrier_t* barrier;
} reducer_args;

/**
 * Function to check if a number can be obtained
 * from another number raised to the exponent given as a parameter.
 * Algorithm inspired from: https://www.reddit.com/r/algorithms/comments/al8y5h/whats_the_most_efficient_way_to_see_if_a_number/
 * and modified to fit the current case 
 */
bool check_power(int n, int exponent) {
    if (pow(2, exponent) > n) {
        return false;
    }

    int lo = 2;
    int hi = lo;
    while (pow(hi, exponent) <= n) {
        hi *= 2;
    }
    while (hi - lo > 1) {
        int middle = (lo + hi) / 2;
        if (pow(middle, exponent) <= n) {
            lo = middle;
        } else {
            hi = middle;
        }
    }

    if (pow(lo, exponent) == n) {
        return true;
    }
	return false;
}

/* --- Function for mappers --- */
void* mapper_func(void *args) {
	int number, numbers_count;
	mapper_args* arg = (mapper_args*)args;
	string current_file;

	/* --- Allocating files to the mapper dynamically --- */
	while ((arg->file_list)->size() > 0) {
		pthread_mutex_lock(arg->mutex);
		
		if ((arg->file_list)->size() > 0) {
			current_file = (arg->file_list)->front();
			(arg->file_list)->pop();
			pthread_mutex_unlock(arg->mutex);
			/* --- Open the file and check if the numbers can be written as a power --- */
			ifstream in(current_file);
			in>>numbers_count;
			for (int i = 0; i < numbers_count; i ++) {
				in>>number;
				for (int j = 2; j <= arg->max_pow; j ++) {
					if(check_power(number, j) || number == 1) {
						(*arg->pow_list)[arg->id][j-2].push_back(number);
					}
				}
			}
			in.close();
		}	
	}

	pthread_barrier_wait(arg->barrier);
	pthread_exit(NULL);
}

/* --- Function for reducers --- */
void* reducer_func(void *args) {
	reducer_args* arg = (reducer_args*)args;
	
	pthread_barrier_wait(arg->barrier);
	
	string filename = "out";
	filename += to_string(arg->id + 2) + ".txt";
	unordered_set<int> numbers;
	ofstream out(filename);

	/* --- Parse every number and add unique values in a set --- */
	for (int i = 0; i < arg->mapper_cnt; i ++) {	
		for(unsigned long j = 0; j < (*arg->pow_list)[i][arg->id].size(); j ++) {
			numbers.insert((*arg->pow_list)[i][arg->id][j]);
		}
	}

	out<<numbers.size();
	out.close();

	pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
	int mapper_cnt, reducer_cnt;
	string test_file;
	queue<string> files;

	/* --- Read args --- */
	if(argc < 4) {
		cout<<"Insufficient parameters5: ./tema1 <mapper_count> <reducer_count> <in_file>\n";
		exit(1);
	}
	mapper_cnt = atoi(argv[1]);
	reducer_cnt = atoi(argv[2]);
	test_file = argv[3];

	/* --- Read files --- */
	int files_count;
	ifstream in(test_file);
	in>>files_count;
	string numbers_file;
	for (int i = 0; i < files_count; i ++) {
		in>>numbers_file;
		files.push(numbers_file);
	}
	in.close();

	/* --- Init Mutex --- */
	pthread_mutex_t mutex;
	pthread_mutex_init(&mutex, NULL);

	/* --- Init Barrier --- */
	pthread_barrier_t barrier;
	pthread_barrier_init(&barrier, NULL, mapper_cnt + reducer_cnt);

	/* --- Init Pow Vector --- */
	vector<vector<vector<int>>> pow_list;
	pow_list.resize(mapper_cnt * sizeof(int));
	for(int i = 0; i < mapper_cnt; i ++) {
		pow_list[i].resize((reducer_cnt + 1) * sizeof(int));
	}

	pthread_t mappers[mapper_cnt], reducers[reducer_cnt];
	
	/* --- Init args for Mappers and Reducers --- */
	mapper_args mapper_arg[mapper_cnt];
	reducer_args reducer_arg[reducer_cnt];

	for(int i = 0; i < mapper_cnt + reducer_cnt; i ++) {
		if (i < mapper_cnt) {
			mapper_arg[i].id = i;
			mapper_arg[i].max_pow = reducer_cnt + 1;
			mapper_arg[i].file_list = &files;
			mapper_arg[i].pow_list = &pow_list;
			mapper_arg[i].mutex = &mutex;
			mapper_arg[i].barrier = &barrier;
			pthread_create(&mappers[i], NULL, mapper_func, &mapper_arg[i]);
		} else {
			reducer_arg[i - mapper_cnt].id = i - mapper_cnt;
			reducer_arg[i - mapper_cnt].mapper_cnt = mapper_cnt;
			reducer_arg[i - mapper_cnt].pow_list = &pow_list;
			reducer_arg[i - mapper_cnt].mutex = &mutex;
			reducer_arg[i - mapper_cnt].barrier = &barrier;
			pthread_create(&reducers[i - mapper_cnt], NULL, reducer_func, &reducer_arg[i - mapper_cnt]);
		}
	}

	void* status;
	for(int i = 0; i < mapper_cnt + reducer_cnt; i ++) {
		if (i < mapper_cnt) {
			pthread_join(mappers[i], &status);
		} else {
			pthread_join(reducers[i - mapper_cnt], &status);
		}
	}

	pthread_mutex_destroy(&mutex);
	pthread_barrier_destroy(&barrier);
	return 0;
}

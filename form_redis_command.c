#include <stdio.h>

int main(){
    FILE *fp;
    if((fp = fopen("redis_command.txt", "w")) == NULL){
        printf("can not open file");
    }
    char str[100];
    for(int i = 0; i < 1000000; ++i){
        fprintf(fp, "SET key%d value%d\n", i, i);
    }
    fclose(fp);

    return 1;
}

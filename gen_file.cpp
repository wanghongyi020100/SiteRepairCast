#include <bits/stdc++.h>
using namespace std;

int main(int argc,char **argv)
{
    if(argc<3)
    {
        cerr<<"usage: "<<argv[0]<<" <size_mb> <output_file>\n";
        return 1;
    }

    long long n=stoll(argv[1]);
    string file=argv[2];

    ofstream out(file,ios::binary);
    if(!out)
    {
        cerr<<"open file failed\n";
        return 1;
    }

    const size_t block_size=1024*1024;
    vector<char>buffer(block_size);

    mt19937 rng(random_device{}());
    uniform_int_distribution<int>dist(0,255);

    for(auto &x:buffer)x=static_cast<char>(rng()%26+'a');

    for(long long i=0;i<n;i++)
    {
        out.write(buffer.data(),buffer.size());
    }

    out.close();

    cout<<"generated "<<n<<" MB file: "<<file<<"\n";
    return 0;
}
package org.example;
import java.util.*;
import java.io.FileReader;
import java.io.*;
import java.util.concurrent.TimeUnit;

import com.opencsv.*;

class nft_data {
    String txn_hash, unix_Times, date_Time, action, buyer, NFT, token_id, type, quantity, price, market;
    int frequency;

    public nft_data()
    {

    }

    public nft_data(String txn_hash, String unix_Times, String date_Time, String action, String buyer, String NFT, String token_id, String type, String quantity, String price, String market) {
        this.txn_hash = txn_hash;
        this.unix_Times = unix_Times;
        this.date_Time = date_Time;
        this.action = action;
        this.buyer = buyer;
        this.NFT = NFT;
        this.token_id = token_id;
        this.type = type;
        this.quantity = quantity;
        this.price = price;
        this.market = market;
    }



    public String getTxn_hash() {
        return txn_hash;
    }

    public void set_frequency(int frequency) {
        this.frequency = frequency;
    }

    public int getFrequency() {
        return frequency;
    }
}

class frequency_data extends nft_data{
    int frequency;
    ArrayList<token_data> token_data;
    frequency_data(int frequency, ArrayList<token_data> token_data)
    {
        this.frequency=frequency;
        this.token_data=token_data;
    }
    public ArrayList<token_data> getTokenData()
    {
        return this.token_data;
    }
    public void setTokenData(ArrayList<token_data> token_data)
    {
        this.token_data = token_data;
    }

}

class token_data extends nft_data{

    String token_id;
    ArrayList<nft_data> nftData = new ArrayList<nft_data>();

    token_data(String token_id, ArrayList<nft_data> nftData)
    {
        this.token_id = token_id;
        this.nftData = nftData;
    }
    public ArrayList<nft_data> getNftData()
    {
        return this.nftData;
    }
    public void setNftData(ArrayList<nft_data> nft_data)
    {
        this.nftData = nft_data;
    }
}

public class Main {


    static int partition(ArrayList<frequency_data> a, int l, int r) {
        int pivot = a.get(r).frequency;
        int i = l-1;
        for (int j= l;j<=r-1;j++)
        {
            if(a.get(j).frequency < pivot)
            {
                i++;
                frequency_data temp = a.get(i);
                a.set(i,a.get(j));
                a.set(j,temp);
            }
        }
        frequency_data temp1 = a.get(i+1);
        a.set(i+1, a.get(r));
        a.set(r,temp1);
        return i+1;

    }

    static ArrayList<frequency_data> quick(ArrayList<frequency_data> a, int start, int end) {
        if (start < end) {
            int p = partition(a, start, end);
            a = quick(a, start, p - 1);
            a = quick(a, p + 1, end );
        }
        return a;
    }
    public static ArrayList<frequency_data> readData(ArrayList<String> file_names,int no_of_records) {
        try {
            int count_records=0;
            ArrayList<nft_data> data_matrix = new ArrayList<nft_data>();
            ArrayList<token_data> token_list = new ArrayList<token_data>();
            ArrayList<Integer> frequency = new ArrayList<Integer>();
            HashMap<String, Integer> find_freq = new HashMap<String, Integer>();
            HashMap<String, String> find_token = new HashMap<String, String>();
            HashMap<String,token_data> token_list_map = new HashMap<String,token_data>();
            for(String file : file_names){
                if(count_records>no_of_records)
                    break;
            FileReader filereader = new FileReader(file);

            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader);

            String[] data = csvReader.readNext();
            while ((data = csvReader.readNext()) != null && count_records<=no_of_records) {
                count_records++;
                data[6] = data[6].replaceAll("[^\\d.]","");
                if (find_freq.containsKey(data[6])) {
                    int temp = find_freq.get(data[6]);
                    temp++;
                    find_freq.put(data[6], temp);
                    token_data t = token_list_map.get(data[6]);
                    ArrayList<nft_data> token_nft_list = t.getNftData();
                    nft_data data_object1 = new nft_data(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10]);
                    token_nft_list.add(data_object1);
                    token_list_map.get(data[6]).setNftData(token_nft_list);
                    token_list_map.put(data[6],token_list_map.get(data[6]));

                } else {
                    ArrayList<nft_data> token_nft_list = new ArrayList<nft_data>();
                    nft_data data_object1 = new nft_data(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10]);
                    token_nft_list.add(data_object1);
                    token_data t=new token_data(data[6],token_nft_list);
                    token_list_map.put(data[6],t);
                    find_freq.put(data[6], 1);
                }
                nft_data data_object = new nft_data(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10]);

                data_matrix.add(data_object);
            } }
            HashMap<Integer,frequency_data> unique = new HashMap<Integer,frequency_data>();
            ArrayList<frequency_data> final_data = new ArrayList<frequency_data>();
            for(String s: token_list_map.keySet())
            {
                int obj_freq = find_freq.get(s);
                if(unique.containsKey(obj_freq))
                {
                    ArrayList<token_data> token_objects = unique.get(obj_freq).getTokenData();
                    token_objects.add(token_list_map.get(s));
                    unique.get(obj_freq).setTokenData(token_objects);
                    unique.put(obj_freq,unique.get(obj_freq));

                }
                else
                {
                    ArrayList<token_data> token_objects = new ArrayList<token_data>();
                    token_objects.add(token_list_map.get(s));
                    frequency_data freq_objects = new frequency_data(obj_freq,token_objects);
                    unique.put(obj_freq,freq_objects);
                }

            }
            for(int i : unique.keySet())
            {
                final_data.add(unique.get(i));
            }
         /*  for (nft_data m : data_matrix) {
                int obj_freq = find_freq.get(m.token_id);
                m.set_frequency(obj_freq);
                if(unique.containsKey(obj_freq))
                {


                    ArrayList<nft_data> nft_objects = unique.get(obj_freq).getNftData();
                    nft_objects.add(m);
                    unique.get(obj_freq).setNftData(nft_objects);
                    unique.put(obj_freq,unique.get(obj_freq));

                }
                else
                {
                    ArrayList<nft_data> nft_objects = new ArrayList<nft_data>();
                    nft_objects.add(m);
                    frequency_data freq_objects = new frequency_data(obj_freq,nft_objects);
                    unique.put(obj_freq,freq_objects);
                }

            }*/
            //quick(data_matrix, 0, data_matrix.size()-1);

            return final_data;
         }catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList<frequency_data> k = new ArrayList<>();
        return k;
    }
    public static ArrayList<frequency_data> insertionsort(ArrayList<frequency_data> a)
    {
        for(int i=1;i<a.size();i++)
        {
            frequency_data key = a.get(i);
            for(int j = i-1;j>=0;j--)
            {
                if(a.get(j).frequency>key.frequency)
                {
                    a.set(j+1, a.get(j));
                }
                else
                {
                    a.set(j+1,key);
                    break;}
            }

        }
        return a;
    }
    public static int getmaxrecords(ArrayList<String> files)
    {
        try{
        int count=0;
        for(String s: files)
        {
            FileReader filereader1 = new FileReader(s);

            // create csvReader object passing
            // file reader as a parameter
            CSVReader csvReader = new CSVReader(filereader1);

            String[] data = csvReader.readNext();
            while ((data = csvReader.readNext()) != null) {
                count++;
            }
        }
        return count;}
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) throws FileNotFoundException{
        String folder_name = "./";
        File f = new File(folder_name);
        ArrayList<String> file_names = new ArrayList<String>();

        for(String s: f.list())
        {
            if(s.endsWith(".csv"))
            {
                file_names.add(folder_name + s);
            }
        }
        int no_of_records=2147483647;
        ArrayList<frequency_data> a = readData(file_names,no_of_records);
        long sort_begin = System.nanoTime();
        a = quick(a,0,a.size()-1);
        long sort_end= System.nanoTime();
        PrintStream o = new PrintStream(new File(folder_name+"Query1_output.txt"));

        PrintStream console = System.out;

       // Output: Token hash (frequency = ?)
       // Token ID: Txn hash, Date Time (UTC), Buyer, NFT, Type, Quantity, Price
        for (int i=a.size()-1;i>=0;i--) {

            ArrayList<token_data> t_nft = a.get(i).getTokenData();
            for(token_data t : t_nft){
                int first=0;
                ArrayList<nft_data> nft = t.getNftData();
            for(nft_data m : nft) {
                if(first==0)
                {
                    System.setOut(o);
                    System.out.println("Token_Id "+ m.token_id + " frequency=" + a.get(i).frequency);
                    System.setOut(console);

                    System.setOut(o);
                    System.out.println("Token ID: Txn hash, Date Time (UTC), Buyer, NFT, Type, Quantity, Price");
                    System.setOut(console);
                }
                System.setOut(o);
                System.out.print(m.token_id + ": ");
                System.setOut(console);

                System.setOut(o);
                System.out.print(m.txn_hash + ", ");
                System.setOut(console);

                System.setOut(o);
                System.out.print(m.date_Time + ", ");
                System.setOut(console);

                System.setOut(o);
                System.out.print(m.buyer + ", ");
                System.setOut(console);

                System.setOut(o);
                System.out.print(m.NFT + ", ");
                System.setOut(console);

                System.setOut(o);
                System.out.print(m.type + ", ");
                System.setOut(console);

                System.setOut(o);
                System.out.print(m.quantity + ", ");
                System.setOut(console);

                System.setOut(o);
                System.out.println(m.price + " ");
                System.setOut(console);
                first++;

            }}
            System.setOut(o);
            System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////");
            System.setOut(console);
        }

       long time_taken_to_sort = TimeUnit.MILLISECONDS.convert(sort_end-sort_begin,TimeUnit.NANOSECONDS);
       System.out.println("Time taken to sort using quick sort in nanoseconds is:" + (sort_end-sort_begin));
       System.out.println("Time taken to sort using quick sort in milliseconds is:" + time_taken_to_sort);

       //insertion sort time calculation
        a = readData(file_names,no_of_records);
        sort_begin = System.nanoTime();
        a = insertionsort(a);
        sort_end= System.nanoTime();
        time_taken_to_sort = TimeUnit.MILLISECONDS.convert(sort_end-sort_begin,TimeUnit.NANOSECONDS);
        System.out.println("Time taken to sort using insertion sort in nanoseconds is:" + (sort_end-sort_begin));
        System.out.println("Time taken to sort using insertion sort in milliseconds is:" + time_taken_to_sort);

       //Graph Calculation
      /*  PrintStream o1 = new PrintStream(new File(folder_name+"graph.txt"));

        PrintStream console1 = System.out;
        int max = getmaxrecords(file_names);
        System.out.println(max);
        for(int i=1000;i<=max;i=i+1000)
        {
            int j=0;
            long avg_time=0;
            System.setOut(o1);
            System.out.print(i+",");
            System.setOut(console);
            ArrayList<frequency_data> a1 = readData(file_names,i);
            while(j<=100)
            {
                ArrayList<frequency_data> b=new ArrayList<>();
                b.addAll(a1);
                sort_begin = System.nanoTime();
                quick(b,0,b.size()-1);
                sort_end= System.nanoTime();
                time_taken_to_sort = sort_end-sort_begin;
                avg_time = avg_time+time_taken_to_sort;
                j++;
            }
            avg_time = avg_time/100;
            System.setOut(o1);
            System.out.println(avg_time);
            System.setOut(console);
        }*/

    }
}

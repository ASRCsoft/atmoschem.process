//Copyright (c) 2011 ashelly.myopenid.com under <http://www.opensource.org/licenses/mit-license>
 
#include <stdlib.h>
 
//Customize for your data Item type
typedef float8 MedItem;
#define ItemLess(a,b)  ((a)<(b))
#define ItemMean(a,b)  (((a)+(b))/2)
 
typedef struct Mediator_t
{
   MedItem* data;  //circular queue of values
   int*  pos;   //index into `heap` for each value
   int*  heap;  //max/median/min heap holding indexes into `data`.
   int   N;     //allocated size.
   int   idx;   //position in circular queue
   int   ct;    //count of items in queue
} Mediator;
 
/*--- Helper Functions ---*/
 
#define minCt(m) (((m)->ct-1)/2) //count of items in minheap
#define maxCt(m) (((m)->ct)/2)   //count of items in maxheap 
 
//returns 1 if heap[i] < heap[j]0
int mmless(Mediator* m, int i, int j)
{
   return ItemLess(m->data[m->heap[i]],m->data[m->heap[j]]);
}
 
//swaps items i&j in heap, maintains indexes
int mmexchange(Mediator* m, int i, int j)
{
   int t = m->heap[i];
   m->heap[i]=m->heap[j];
   m->heap[j]=t;
   m->pos[m->heap[i]]=i;
   m->pos[m->heap[j]]=j;
   return 1;
}
 
//swaps items i&j if i<j;  returns true if swapped
int mmCmpExch(Mediator* m, int i, int j)
{
   return (mmless(m,i,j) && mmexchange(m,i,j));
}
 
//maintains minheap property for all items below i/2.
void minSortDown(Mediator* m, int i)
{
   for (; i <= minCt(m); i*=2)
   {  if (i>1 && i < minCt(m) && mmless(m, i+1, i)) { ++i; }
      if (!mmCmpExch(m,i,i/2)) { break; }
   }
}
 
//maintains maxheap property for all items below i/2. (negative indexes)
void maxSortDown(Mediator* m, int i)
{
   for (; i >= -maxCt(m); i*=2)
   {  if (i<-1 && i > -maxCt(m) && mmless(m, i, i-1)) { --i; }
      if (!mmCmpExch(m,i/2,i)) { break; }
   }
}
 
//maintains minheap property for all items above i, including median
//returns true if median changed
int minSortUp(Mediator* m, int i)
{
   while (i>0 && mmCmpExch(m,i,i/2)) i/=2;
   return (i==0);
}
 
//maintains maxheap property for all items above i, including median
//returns true if median changed
int maxSortUp(Mediator* m, int i)
{
   while (i<0 && mmCmpExch(m,i/2,i))  i/=2;
   return (i==0);
}

int firstNullAtCt(int ct) {
  return(((ct+1)/2) * ((ct&1)?-1:1));
}
 
/*--- Public Interface ---*/
 
 
//creates new Mediator: to calculate `nItems` running median. 
//mallocs single block of memory, caller must free.
Mediator* MediatorNew(int nItems)
{
   int size = sizeof(Mediator)+nItems*(sizeof(MedItem)+sizeof(int)*2);
   Mediator* m=  malloc(size);
   m->data= (MedItem*)(m+1);
   m->pos = (int*) (m->data+nItems);
   m->heap = m->pos+nItems + (nItems/2); //points to middle of storage.
   m->N=nItems;
   m->ct = m->idx = 0;
   while (nItems--)  //set up initial heap fill pattern: median,max,min,max,...
   {  m->pos[nItems]= firstNullAtCt(nItems);
      m->heap[m->pos[nItems]]=nItems;
   }
   return m;
}

int MediatorPosIsNull(Mediator* m, int p)
{
   // everything's null if there's nothing there
   if (m->ct == 0) return(1);
   // it's null if it's outside the min and max heaps
   return((p < -maxCt(m)) || (p > minCt(m)));
}

//Removes the item at index p, maintains median
void MediatorPopIdx(Mediator* m, int p)
{
   /* following conversation at https://stackoverflow.com/a/5970314 */
   if (MediatorPosIsNull(m, p)) {
     // no popping required, skip all this
     return;
   }
   // index to swap the old value with (will be counted as null
   // after the count is reduced)
   int p_null = firstNullAtCt(m->ct - 1);
   
   int switches_sides = (p>0) != (p_null>0);
   mmexchange(m, p, p_null);
   m->ct--;
   if (p>0)         //item was in minHeap
   {
     if (switches_sides) {
       minSortUp(m,p);
       maxSortDown(m,-1);
     } else {
       minSortDown(m,p*2);
     };
   }
   else if (p<0)   //item was in maxheap
   {
     if (switches_sides) {
       maxSortUp(m,p);
       minSortDown(m,-1);
     } else {
       maxSortDown(m,p*2);
     };
   }
   else            //item was at median
   {
     // only need to sort the one side
     if (p_null>0) {
       minSortDown(m, 1);
     } else if (p_null<0) {
       maxSortDown(m,-1);
     }
     // if p_null==0 then no sorting needs to be done (it was the only
     // value)
   }
}

//Removes the oldest item, maintains median
void MediatorPopOldest(Mediator* m)
{
   /* following conversation at https://stackoverflow.com/a/5970314 */
   // oldest non-null value
   int p = m->pos[(m->idx - m->ct + m->N) % m->N];
   MediatorPopIdx(m, p);
}

//Inserts item, maintains median in O(lg nItems)
void MediatorInsertNonNull(Mediator* m, MedItem v, int idx)
{
   int p = m->pos[idx];
   // isNew: replaced item is a null
   int isNew = MediatorPosIsNull(m, p);
   if (isNew) {
     // put the new number at the next empty (null) value
     p = firstNullAtCt(m->ct);
     mmexchange(m, p, m->pos[idx]);
   }
   MedItem old = m->data[idx];
   m->data[idx]=v;
   m->ct+=isNew;
   if (p>0)         //new item is in minHeap
   {  if (!isNew && ItemLess(old,v)) { minSortDown(m,p*2);  }
      else if (minSortUp(m,p)) { maxSortDown(m,-1); }
   }
   else if (p<0)   //new item is in maxheap
   {  if (!isNew && ItemLess(v,old)) { maxSortDown(m,p*2); }
      else if (maxSortUp(m,p)) { minSortDown(m, 1); }
   }
   else            //new item is at median
   {  if (maxCt(m)) { maxSortDown(m,-1); }
      if (minCt(m)) { minSortDown(m, 1); }
   }
}

//Inserts item, maintains median in O(lg nItems)
void MediatorInsert(Mediator* m, MedItem v, int isnull)
{
   int p = m->pos[m->idx];
   if (isnull) {
     MediatorPopIdx(m, p);
   } else {
     MediatorInsertNonNull(m, v, m->idx);
   }
   // update the index
   m->idx = (m->idx+1) % m->N;
}
 
//returns median item (or average of 2 when item count is even)
MedItem MediatorMedian(Mediator* m)
{
   MedItem v= m->data[m->heap[0]];
   if ((m->ct&1)==0) { v= ItemMean(v,m->data[m->heap[-1]]); }
   return v;
}
 
 
/*--- Test Code ---*/
/* #include <stdio.h>
void PrintMaxHeap(Mediator* m)
{
   int i;
   if(maxCt(m))
      printf("Max: %3f",m->data[m->heap[-1]]);
   for (i=2;i<=maxCt(m);++i)
   {
      printf("|%3f ",m->data[m->heap[-i]]);
      if(++i<=maxCt(m)) printf("%3f",m->data[m->heap[-i]]);
   }
   printf("\n");
}
void PrintMinHeap(Mediator* m)
{
   int i;
   if(minCt(m))
      printf("Min: %3f",m->data[m->heap[1]]);
   for (i=2;i<=minCt(m);++i)
   {
      printf("|%3f ",m->data[m->heap[i]]);
      if(++i<=minCt(m)) printf("%3f",m->data[m->heap[i]]);
   }
   printf("\n");
}
 
void ShowTree(Mediator* m)
{
   PrintMaxHeap(m);
   printf("Mid: %3f\n",m->data[m->heap[0]]);
   PrintMinHeap(m);
   printf("\n");
}
 
int main(int argc, char* argv[])
{
   int i,isnull;
   float v;
   Mediator* m = MediatorNew(15);
 
   for (i=1;i<=5;i++)
   {
      // v = rand()&127;
      v = i;
      isnull = i == 1;
      printf("Inserting %3f \n",v);
      MediatorInsert(m,v,isnull);
      v=MediatorMedian(m);
      printf("Median = %3f.\n\n",v);
      ShowTree(m);
   }
} */

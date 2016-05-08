#include <iostream>
#include <vector>

#include "geeps.hpp"

using std::vector;
using std::cout;
using std::endl;

int main() {
  /* Create a GeePs object with a GeePsConfig */
  GeePsConfig geeps_config;
  geeps_config.host_list.push_back("localhost");
  int machine_id = 0;
  geeps_config.gpu_memory_capacity = (size_t)1 << 32;
      /* Set GPU memory capacity to 4 GB */
  GeePs *geeps = new GeePs(machine_id, geeps_config);

  /* Prepare the row keys */
  size_t table_id = 0;
  int slack = 0;    /* BSP mode */
  vector<size_t> row_ids;
  row_ids.push_back(0);
      /* Only one row, with ID 0 */

  /* Perform virtual iteration */
  int read_handle = geeps->VirtualRead(table_id, row_ids, slack);
  int preupdate_handle = geeps->VirtualPreUpdate(table_id, row_ids);
  int postread_handle = geeps->VirtualPostRead(read_handle);
  int update_handle = geeps->VirtualUpdate(preupdate_handle);
  geeps->VirtualClock();
  geeps->FinishVirtualIteration();

  /* After the FinishVirtualIteration() function is called,
   * you can make your reported access with the handles.
   * Before you calling the StartIterations() function,
   * you can do the reported accesses in whatever orders you want. */
  /* For example, here, we can set some initial values to our parameter data */
  /* First request an update buffer (in GPU memory) */
  RowData *update_buffer;
  geeps->PreUpdate(preupdate_handle, &update_buffer);
  /* Suppose you have filled the update into the update buffer */
  /* Call the Update() function to release the buffer and finish this update */
  geeps->Update(update_handle);
  /* Signal the completion of a clock */
  geeps->Clock();

  geeps->StartIterations();
  /* After the StartIterations() function is called,
   * you can only issue the GeePS calls with the order you reported
   * at the virtual iteration. */
  /* Start the iterations */
  for (int clock = 0; clock < 10; clock++) {
    /* Read the data */
    RowData *read_buffer;
    geeps->Read(read_handle, &read_buffer);
    /* Current data is read in read buffer */
    /* Update the data */
    RowData *update_buffer;
    geeps->PreUpdate(preupdate_handle, &update_buffer);
    /* Suppose you have filled the update into the update buffer */
    /* Call the PostRead() function to release the buffer */
    geeps->PostRead(postread_handle);
    geeps->Update(update_handle);
    /* Signal the completion of a clock */
    geeps->Clock();
  }

  cout << "Finished \"training\", hello world!\n\n";
  delete geeps;
}

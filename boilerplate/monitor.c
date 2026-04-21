/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Provided boilerplate:
 *   - device registration and teardown
 *   - timer setup
 *   - RSS helper
 *   - soft-limit and hard-limit event helpers
 *   - ioctl dispatch shell
 *
 * YOUR WORK: Fill in all sections marked // TODO.
 */

#include <linux/cdev.h>// character device registration
#include <linux/device.h> // device creation
#include <linux/fs.h> // file operations
#include <linux/kernel.h> // printk and other kernel utilities
#include <linux/list.h> // linked list implementation
#include <linux/mm.h> // memory management and RSS calculation
#include <linux/module.h> // module macros
#include <linux/mutex.h> // mutexes for synchronization
#include <linux/pid.h> // PID handling
#include <linux/sched/signal.h> // sending signals to tasks
#include <linux/slab.h> // kernel memory allocation
#include <linux/timer.h> // timers for periodic checks
#include <linux/uaccess.h> // copy_to_user and copy_from_user
#include <linux/version.h>  // for kernel version checks

#include "monitor_ioctl.h" // ioctl command definitions and monitor_request struct

#define DEVICE_NAME "container_monitor" //This is the name of the device file.
#define CHECK_INTERVAL_SEC 1 //Monitor checks memory every 1 second.

/* ==============================================================
 * TODO 1: Define your linked-list node struct.
 *
 * Requirements:
 *   - track PID, container ID, soft limit, and hard limit
 *   - remember whether the soft-limit warning was already emitted
 *   - include `struct list_head` linkage
 * ============================================================== */

struct monitor_entry {
    pid_t pid;
    char container_id[MONITOR_NAME_LEN];
    unsigned long soft_limit;
    unsigned long hard_limit;
    int soft_triggered; // flag to track if soft limit warning has been emitted
    struct list_head list; // linked list node
};


/* ==============================================================
 * TODO 2: Declare the global monitored list and a lock.
 *
 * Requirements:
 *   - shared across ioctl and timer code paths
 *   - protect insert, remove, and iteration safely
 *
 * You may choose either a mutex or a spinlock, but your README must
 * justify the choice in terms of the code paths you implemented.
 * ============================================================== */
static LIST_HEAD(monitor_list); //Creates a global linked list head called monitor_list.
static DEFINE_MUTEX(monitor_lock); //Creates a mutex lock named monitor_lock.


/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num; //Stores device number assigned by kernel.
static struct cdev c_dev; //Character device structure for registering our device and its file operations.
static struct class *cl; //Device class pointer, used for /dev creation.

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 *
 * Returns the Resident Set Size in bytes for the given PID,
 * or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task; //kernel representation of a process
    struct mm_struct *mm; //process memory description
    long rss_pages = 0; //memory count in pages

    rcu_read_lock(); //Starts an RCU read-side critical section.
    task = pid_task(find_vpid(pid), PIDTYPE_PID); //Looks up the task_struct for the given PID. If the process has exited, this will return NULL.
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task); //Increments the reference count of the task_struct to ensure it doesn't get freed while we're using it.
    rcu_read_unlock(); //End RCU read section.

    mm = get_task_mm(task); //Get the memory descriptor of the process.
    if (mm) {
        rss_pages = get_mm_rss(mm); //get RSS page count
        mmput(mm);
    }
    put_task_struct(task); //Release reference to task.

    return rss_pages * PAGE_SIZE; //convert pages to bytes and return.
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 *
 * Log a warning when a process exceeds the soft limit.
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 *
 * Kill a process when it exceeds the hard limit.
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID); //find task from PID safely.
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Timer Callback - fires every CHECK_INTERVAL_SEC seconds.
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t) //This function runs every 1 second.
{
    /* ==============================================================
     * TODO 3: Implement periodic monitoring.
     *
     * Requirements:
     *   - iterate through tracked entries safely
     *   - remove entries for exited processes
     *   - emit soft-limit warning once per entry
     *   - enforce hard limit and then remove the entry
     *   - avoid use-after-free while deleting during iteration
     * ============================================================== */

struct monitor_entry *entry, *tmp;

mutex_lock(&monitor_lock); //Lock the shared list before accessing it.

list_for_each_entry_safe(entry, tmp, &monitor_list, list) { //Safely iterate through the linked list of monitored entries, allowing for deletion of entries while iterating.

    long rss = get_rss_bytes(entry->pid); //Get current RSS of this PID.

    if (rss < 0) { //If get_rss_bytes returns -1, the process no longer exists. We should remove this entry from the list and free its memory.
        list_del(&entry->list);
        kfree(entry);
        continue;
    }

    if (rss > entry->soft_limit && !entry->soft_triggered) { //If memory is above soft limit and warning has not already been printed:
        log_soft_limit_event(entry->container_id, entry->pid,
                             entry->soft_limit, rss);
        entry->soft_triggered = 1;
    }

    if (rss > entry->hard_limit) { //If memory crossed hard limit
        kill_process(entry->container_id, entry->pid,
                     entry->hard_limit, rss);

        list_del(&entry->list);
        kfree(entry);
    }
}

mutex_unlock(&monitor_lock);


    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ); //Reschedule timer to run again after 1 second.

}

/* ---------------------------------------------------------------
 * IOCTL Handler
 *
 * Supported operations:
 *   - register a PID with soft + hard limits
 *   - unregister a PID when the runtime no longer needs tracking
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
 //This is the function called when user-space does an ioctl() on /dev/container_monitor.

{
    struct monitor_request req; //This struct will hold the data passed from user-space, including container ID, PID, and limits. We will copy data into this struct from user-space using copy_from_user().

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes); //Log the registration request.

        /* ==============================================================
         * TODO 4: Add a monitored entry.
         *
         * Requirements:
         *   - allocate and initialize one node from req
         *   - validate allocation and limits
         *   - insert into the shared list under the chosen lock
         * ============================================================== */
	struct monitor_entry *entry = kmalloc(sizeof(*entry), GFP_KERNEL); //Allocate memory in kernel for one new list node.
    
    if (!entry)
        return -ENOMEM;

    entry->pid = req.pid;
    entry->soft_limit = req.soft_limit_bytes;
    entry->hard_limit = req.hard_limit_bytes;
    entry->soft_triggered = 0;
    strncpy(entry->container_id, req.container_id, MONITOR_NAME_LEN); //Copy container name into the entry.


    mutex_lock(&monitor_lock);
    list_add(&entry->list, &monitor_list); //inserts at beginning
    mutex_unlock(&monitor_lock);

    return 0;

        
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    /* ==============================================================
     * TODO 5: Remove a monitored entry on explicit unregister.
     *
     * Requirements:
     *   - search by PID, container ID, or both
     *   - remove the matching entry safely if found
     *   - return status indicating whether a matching entry was removed
     * ============================================================== */
     struct monitor_entry *entry, *tmp;

mutex_lock(&monitor_lock);

list_for_each_entry_safe(entry, tmp, &monitor_list, list) { //Go through all monitored entries.
    if (entry->pid == req.pid) {
        list_del(&entry->list);
        kfree(entry);
        mutex_unlock(&monitor_lock);
        return 0;
    }
}

    mutex_unlock(&monitor_lock);
    return -ENOENT;

    }

/* --- Provided: file operations --- */
static struct file_operations fops = { //This connects  device file to handler functions.
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void) //runs when module is loaded using insmod.
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0) //Ask kernel to allocate a character device number.
        return -1;
//This creates the device class.
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops); //Initialize the character device structure with our file operations.
    if (cdev_add(&c_dev, dev_num, 1) < 0) { //Add the character device to kernel.
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ); //Start the first timer event 1 second from now.

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void) //Runs when module is unloaded: sudo rmod monitor
{
    del_timer_sync(&monitor_timer); //stop timer safely

    /* ==============================================================
     * TODO 6: Free all remaining monitored entries.
     *
     * Requirements:
     *   - remove and free every list node safely
     *   - leave no leaked state on module unload
     * ============================================================== */
      struct monitor_entry *entry, *tmp;

mutex_lock(&monitor_lock);

list_for_each_entry_safe(entry, tmp, &monitor_list, list) {
    list_del(&entry->list);
    kfree(entry);
}

mutex_unlock(&monitor_lock);

    cdev_del(&c_dev); //Remove character device from kernel.
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");

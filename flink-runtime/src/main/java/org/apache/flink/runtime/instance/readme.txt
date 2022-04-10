一、Instance
代表着一个TaskManager的实例对象 -- 在JobManager上拥有该TaskManager的实例对象

二、slot
1.抽象类Slot定义了该槽位属于哪个TaskManager（instance）的第几个槽位（slotNumber），属于哪个Job（jobID）等信息
2.最简单的情况下，一个slot只持有一个task，也就是SimpleSlot的实现。
3.复杂点的情况，一个slot能共享给多个task使用，也就是SharedSlot的实现。
SharedSlot能包含其他的SharedSlot，也能包含SimpleSlot。所以一个SharedSlot能定义出一棵slots树。

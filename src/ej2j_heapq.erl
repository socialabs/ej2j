% Copyright (c) 2010-2011, Lars Buitinck
% May be used, redistributed and modified under the terms of the
% GNU Lesser General Public License (LGPL), version 2.1 or later

% Heaps/priority queues in Erlang

% Heaps are data structures that return the entries inserted into them in
% sorted order. This makes them the data structure of choice for implementing
% priority queues, a central element of algorithms such as best-first/A*
% search and Kruskal's minimum-spanning-tree algorithm.
%
% This module implements min-heaps, meaning that items are retrieved in
% ascending order of key/priority.
%
% The current version implements pairing heaps. All operations except
% from_list/1, sort/1 and to_list/1 can be performed in at most O(lg n)
% amortized time.
%
% (The actual time complexity of pairing heaps is complicated and not yet
% determined conclusively; see, e.g. S. Pettie (2005), Towards a final
% analysis of pairing heaps, Proc. FOCS'05.)
%
% Ported from an earlier Prolog version, now available as part of SWI-Prolog,
% http://www.swi-prolog.org/pldoc/doc/swi/library/heaps.pl

-module(ej2j_heapq).
-export([add/2, delete_min/1, from_list/1, empty/1, merge/2, new/0, min/1,
         size/1, sort/1, to_list/1]).
-import(lists).


% Public interface: priority queues

% Add element X to priority queue
add({prioq, Heap, N}, X) ->
    {prioq, meld(Heap, {X, []}), N + 1}.

% Return new priority queue with minimum element removed
delete_min({prioq, {_, Sub}, N}) ->
    {prioq, pair(Sub), N - 1}.

% Construct priority queue from list
from_list(L) ->
    lists:foldl(fun(X, Q) -> add(X, Q) end, new(), L).

% True iff argument is an empty priority queue
empty({prioq, nil, 0}) -> true;
empty(_) -> false.

% Merge two priority queues
merge({prioq, Heap0, M}, {prioq, Heap1, N}) ->
    {prioq, meld(Heap0, Heap1), M + N}.

% Get minimum element
min({prioq, Heap, _}) -> heap_min(Heap).

% Returns new, empty priority queue
new() -> {prioq, nil, 0}.

% Number of elements in queue
size({prioq, _, N}) -> N.

% Heap sort a list
sort(L) -> to_list(from_list(L)).

% List elements in priority queue in sorted order
to_list({prioq, nil, _}) -> [];
to_list(Q) ->
    [min(Q) | to_list(delete_min(Q))].


% Guts: pairing heaps
% A pairing heap is either nil or a term {Item, SubHeaps}
% where SubHeaps is a list of heaps.

heap_min({X, _}) -> X.

meld(nil, Q) -> Q;
meld(Q, nil) -> Q;
meld(L = {X, SubL}, R = {Y, SubR}) ->
    if
        X < Y ->
            {X, [R|SubL]};
        true ->
            {Y, [L|SubR]}
    end.

% "Pair up" (recursively meld) a list of pairing heaps.
pair([]) -> nil;
pair([Q]) -> Q;
pair([Q0, Q1 | Qs]) ->
    Q2 = meld(Q0, Q1),
    pair([Q2 | Qs]).

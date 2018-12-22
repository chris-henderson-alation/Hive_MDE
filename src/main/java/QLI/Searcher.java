package QLI;

import java.util.*;

/**
 * Searcher defines a generic graph search and optional backtracking algorithm.
 *
 * The following pseudo code is a recursive representative of the underlying algorithm.
 *
 * let procedure backtrack(root, solution):
 *     if reject(solution, root):
 *         return
 *     solution.append(root)
 *     if accept(solution):
 *         solution.pop()
 *         return
 *     for child ∈ children(root):
 *         backtrack(child, solution)
 *
 * Given a node which generates all possible root nodes of the graph and the following procedures...
 *
 *  children(T node): A procedure which returns an iterator of all of the children of "node"
 *  reject(T[] solution, T node): A procedure which returns whether or not the candidate "node" is valid within
 *                                  the context of the given solution.
 *  accept(T[] solution): A procedure which returns whether or not the give solution is complete.
 *
 * ...any graph search may be conducted.
 *
 * Implementors of Searcher are tasked to define children, accept, and optionally reject. If reject is not overriden,
 * then it will always return "false", which is to say that it will never reject, which will transform the backtracking
 * algorithm to a depth-first search.
 *
 * @param <T>
 */
public abstract class Searcher<T> {

    /**
     * search conducts a depth-first search starting at the given node, named the First Choice Generator (FCG).
     * The FCG is a node from without the graph which generates all possible entry points to the graph. If the graph
     * in question is a tree, then the FCG will only produce only one node - the root node. If the graph in question
     * is a forest, however, then the FCG may produce many roots (think solving a maze where the maze has multiple entraces.)
     *
     * Backtracking is supported via implementation of the reject procedure.
     *
     * @param FCG
     * @return
     */
    public final void search(T FCG) {
        // Backtracking requires a stack. We are going to be consulting the user's
        // "children" procedure to produce for us an iterator which describes all
        // of a given node's children.
        //
        // Rather than utilize the callstack and risk a stack overflow, we are going to allocate our stack to the heap.
        Stack<Iterator<T>> stack = new Stack<>();
        Iterator<T> root = this.children(FCG);
        while (true) {
            // If the root has no further children, then we must
            // consult the stack for the next root to consider.
            if (!root.hasNext()) {
                // However, if the stack is empty as well, then
                // that means that the algorithm is complete.
                if (stack.empty()) {
                    return;
                }
                // Get the next node iterator off the stack.
                root = stack.pop();
                continue;
            }
            // Receive the next candidate node.
            T candidate = root.next();
            // Vis-a-vis what we have seen this far, is this candidate node valid?
            if (this.reject(candidate)) {
                // No, it is not a valid node, continue the search.
                continue;
            }
            // It is a valid node, so append it to the solution thus far.
            // Is this a whole and complete solution?
            if (this.accept(candidate)) {
                // ...and continue searching through other child nodes
                // of the current root node.
                continue;
            }
            // The latest candidate node was valid, but the solution is not
            // yet complete, so push the current root node onto the stack
            // and made the current candidate the new root.
            stack.push(root);
            root = this.children(candidate);
        }
    }

    /**
     * children generates an iterator of all children of the given node.
     *
     * @param node
     * @return
     */
    public abstract Iterator<T> children(final T node);

    /**
     * reject determines whether the given node is valid within the context of
     * the solution computed thus far. Implementing this logic transforms the
     * search procedure into a backtracking algorithm.
     *
     * If the default implementation is used, then all searches will be
     * a full depth-first search.
     *
     * @param solution
     * @param node
     * @return
     */
    public abstract boolean reject(final T node);

    /**
     * accept determines whether the provided solution is complete.
     *
     * @param solution
     * @return
     */
    public abstract boolean accept(final T node);

}


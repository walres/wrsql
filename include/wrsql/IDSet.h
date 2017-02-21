/**
 * \file wrsql/IDSet.h
 *
 * \brief wr::sql::IDSet class declaration
 *
 * \copyright
 * \parblock
 *
 *   Copyright 2014-2017 James S. Waller
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * \endparblock
 */
#ifndef WRSQL_ID_SET_H
#define WRSQL_ID_SET_H

#include <string>
#include <vector>

#include <wrsql/Config.h>
#include <wrsql/Session.h>
#include <wrsql/Statement.h>


namespace wr {


class u8string_view;


namespace sql {

/**
 * \class wr::sql::IDSet
 * \brief SQL-accessible container of integer row IDs
 *
 * \c IDSet provides an ordered random-access container of integer row IDs
 * with STL-like operations similar to that of \c std::set<ID> plus the
 * ability to access it as a temporary table in an SQL database, allowing it
 * to be easily and efficiently used with SQL statements. This temporary table
 * has a single column named \c id, is of type \c INTEGER and is the primary
 * key.
 *
 * Before an \c IDSet object can be used with SQL statements, it must be
 * \e attached to a database connection (<em>i.e.</em> a \c wr::sql::Session
 * object). This causes it to exist within the database as a temporary table
 * with an automatically-generated name retrievable using the \c sql_name()
 * method.
 *
 * \c IDSet objects can be passed as arguments to the \c wr::print() and
 * \c wr::printStr() functions using the \c \%s conversion specifier which
 * inserts the name of the temporary table. This provides a convenient way to
 * build SQL statements.
 */
class WRSQL_API IDSet
{
public:
        using this_t = IDSet;
        using storage_type = std::vector<ID>;
        using size_type = storage_type::size_type;
        using value_type = storage_type::value_type;
        using difference_type = storage_type::difference_type;
        using pointer = storage_type::pointer;
        using reference = storage_type::reference;
        using iterator = storage_type::const_iterator;
        using const_iterator = storage_type::const_iterator;
        using reverse_iterator = storage_type::const_reverse_iterator;
        using const_reverse_iterator = storage_type::const_reverse_iterator;


        ///@{
        /**
         * \brief object constructor
         *
         * Specifying \c db causes the \c IDSet to be attached to the
         * database connection by an implicit call to <code>attach(db)</code>.
         *
         * Specifying an lvalue reference to another \c IDSet instance,
         * \c other, results in the elements or \c other being copied to
         * \c this. If \c other is attached to a database then the new object
         * will be attached to that database also.
         *
         * If \c other is an rvalue reference to another \c IDSet then the
         * contents and state of \c other is transferred to \c *this, and
         * \c other should no longer be used.
         *
         * \param [in,out] other
         *      another instance of \c IDSet to be transferred or copied
         * \param [in] ids
         *      hard-coded list of IDs from source code; ordering does not
         *      matter
         * \param [in] db
         *      connection to the target database
         *
         * \see \c operator=()
         * \see \c attach()
         */
        IDSet();
        IDSet(const this_t &other);
        IDSet(this_t &&other);
        IDSet(std::initializer_list<ID> ids);
        IDSet(Session &db);
        IDSet(Session &db, const this_t &other);
        IDSet(Session &db, std::initializer_list<ID> ids);
        ///@}

        /**
         * \brief object destructor
         *
         * Frees memory allocated to the \c IDSet to hold its elements and
         * if attached to a database, removes the associated temporary table.
         */
        ~IDSet();

        ///@{
        /**
         * \brief assignment operator
         *
         * Specifying an lvalue reference to another \c IDSet instance,
         * \c other, results in the elements of \c other being copied to
         * \c this. If \c this is not attached to any database and \c other
         * is attached to one then \c this will be attached to that database
         * also.
         *
         * If \c other is an rvalue reference to another \c IDSet then the
         * contents and state of \c other is transferred to \c *this, and
         * \c other should no longer be used. In this case any existing
         * iterators referencing elements of \c other remain valid and
         * reference the corresponding values of \c this.
         *
         * If <code>(&other == this)</code> these operator has no effect.
         *
         * \note
         *      Any iterators referencing elements of \c this prior to
         *      assignment must be considered invalid upon return.
         *
         * \param [in,out] other
         *      another instance of \c IDSet to be transferred or copied
         * \param [in] ids
         *      literal list of IDs provided in application source code;
         *      ordering does not matter
         *
         * \return reference to \c *this
         *
         * \see \c attach()
         */
        this_t &operator=(const this_t &other);
        this_t &operator=(this_t &&other);
        this_t &operator=(std::initializer_list<ID> ids);
        ///@}

        /**
         * \brief make an \c IDSet accessible through a database
         *
         * \c attach() associates an \c IDSet object with a database; the
         * \c IDSet object is represented to the database as a temporary
         * table which is accessible to SQL statements, but only on the
         * specified database connection.
         *
         * The exact nature of the temporary table depends on the underlying
         * database implementation. For SQLite3 databases an \c IDSet is
         * implemented as a 'virtual' table which allows the SQLite3 library
         * to access the local element storage directly.
         *
         * An \c IDSet object can be attached to only one database; calling
         * \c attach() will perform an implicit \c detach() on an \c IDSet
         * object that is already attached to another database (and will have
         * no effect if it is already attached to \c db).
         *
         * \param [in] db
         *      connection to the target database
         *
         * \return reference to \c *this
         */
        this_t &attach(Session &db);

        /**
         * \brief remove an IDSet from a previously attached database
         *
         * \c detach() has no effect on an \c IDSet object that is not
         * attached to any database.
         *
         * \return reference to \c *this
         */
        this_t &detach();

        /**
         * \brief retrieve the attached database connection
         * \return pointer to the database connection if attached or
         *      \c false otherwise
         */
        Session *db() const;

        ///@{
        /**
         * \brief insert value(s)
         *
         * \note
         *      Upon successful insertion any iterators referencing elements
         *      of \c this prior to calling \c insert() must be considered
         *      invalid.
         *
         * \param [in] id
         *      single value to insert
         * \param [in] first
         *      iterator referencing first value in source range
         * \param [in] last
         *      iterator following last value in source range
         * \param [in] other
         *      another \c IDSet object containing values to insert
         * \param [in] ids
         *      initializer list specifying values to insert; ordering does
         *      not matter
         * \param [in] stmt
         *      prepared SQL statement to source values from; result ordering
         *      does not matter
         * \param [in] col_no
         *      zero-based target column number to take from results of \c stmt
         * \param [in] sql
         *      an SQL query returning the target values as the first column;
         *      result ordering does not matter
         * \param [in] args
         *      zero or more arguments to be bound to any parameters in \c sql
         *
         * \return
         *      <code>insert(ID)</code> returns a pair of values: the
         *      first is an \c iterator to the place in the container
         *      referencing the value \c id; the second is \c true if \c id
         *      was actually inserted in the container or \c false if \c id
         *      already existed
         * \return
         *      all other overloads return the number of elements inserted,
         *      ignoring those that already existed prior to calling
         *      \c insert()
         */
        std::pair<iterator, bool> insert(ID id);
        size_type insert(const this_t &other);
        size_type insert(std::initializer_list<ID> ids);
        size_type insert(Statement &stmt, int col_no = 0);

        template <typename SrcIter>
                size_type insert(SrcIter first, SrcIter last);

        template <typename ... Args>
                size_type insert_sql(const u8string_view &sql, Args &&...args);
        ///@}

        ///@{
        /**
         * \brief remove element(s)
         *
         * \note
         *      iterators referencing elements of \c this before the call to
         *      erase() may be invalidated.
         *
         * \param [in] id
         *      single value to erase
         * \param [in] pos
         *      iterator to target element
         * \param [in] first
         *      iterator to first target element in range
         * \param [in] last
         *      iterator pointing beyond last target element in range
         * \param [in] other
         *      another \c IDSet object specifying values to erase
         * \param [in] stmt
         *      prepared SQL statement to source values from; result ordering
         *      does not matter
         * \param [in] col_no
         *      zero-based target column number to take from results of \c stmt
         * \param [in] ids
         *      initializer list specifying values to erase; ordering does
         *      not matter
         * \param [in] sql
         *      an SQL query returning the target values as the first column;
         *      result ordering does not matter
         * \param [in] args
         *      zero or more arguments to be bound to any parameters in \c sql
         *
         * \return
         *      \c erase(const_iterator) and <code>erase(const_iterator,
         *      const_iterator)</code> return an iterator to the element
         *      following the last erased element or \c end() if the last
         *      element was removed
         * \return
         *      all other overloads return the number of elements removed
         */
        size_type erase(ID id);
        iterator erase(const_iterator pos);
        iterator erase(const_iterator first, const_iterator last);
        size_type erase(const this_t &other);
        size_type erase(Statement &stmt, int col_no = 0);
        size_type erase(std::initializer_list<ID> ids);

        template <typename ... Args> size_type
                erase_sql(const u8string_view &sql, Args &&...args);
        ///@}

        ///@{
        /**
         * \brief choose common elements of \c this and another set
         *
         * \c intersect() compares the values of an IDSet with those of a
         * secondary set derived from the argument(s) given, removing values
         * from \c this that are not present in the secondary set, thus
         * leaving only the values that the two sets have in common.
         *
         * \note
         *      Any iterators referencing elements of \c this prior to calling
         *      \c intersect() must be considered invalid upon return unless
         *      the return value is zero.
         *
         * \param [in] other
         *      another \c IDSet object containing the secondary set
         * \param [in] stmt
         *      prepared SQL statement that yields the secondary set; <em>the
         *      statement results must be sequenced in ascending order of the
         *      column taken</em>
         * \param [in] col_no
         *      zero-based target column number to take from results of \c stmt
         * \param [in] ids
         *      hard-coded list of values from source code; ordering does not
         *      matter
         * \param [in] first
         *      iterator referencing first value in secondary set
         * \param [in] last
         *      iterator referencing beyond last value in secondary set
         * \param [in] sql
         *      an SQL query returning the target values as the first column;
         *      <em>the results must be sequenced in ascending order of the
         *      first column</em>
         * \param [in] args
         *      zero or more arguments to be bound to any parameters in \c sql
         *
         * \return number of elements removed from \c this
         */
        size_type intersect(const this_t &other);
        size_type intersect(Statement &stmt, int col_no = 0);
        size_type intersect(std::initializer_list<ID> ids);

        template <typename SrcIter> size_type
                intersect(SrcIter first, SrcIter last);

        template <typename ... Args> size_type
                intersect_sql(const u8string_view &sql, Args &&...args);
        ///@}

        ///@{
        /**
         * \brief choose elements not shared between \c this and another set
         *
         * \c symmetric_difference() compares the values of an IDSet with
         * those of a secondary set derived from the argument(s) given,
         * removing from \c this any values that the two sets have in common.
         *
         * \note
         *      Any iterators referencing elements of \c this prior to calling
         *      \c symmetric_difference() must be considered invalid upon
         *      return.
         *
         * \param [in] other
         *      another \c IDSet object containing the secondary set
         * \param [in] stmt
         *      prepared SQL statement that yields the secondary set; <em>the
         *      statement results must be sequenced in ascending order of the
         *      column taken</em>
         * \param [in] col_no
         *      zero-based target column number to take from results of \c stmt
         * \param [in] ids
         *      hard-coded list of values from source code
         * \param [in] first
         *      iterator referencing first value in secondary set
         * \param [in] last
         *      iterator referencing beyond last value in secondary set
         * \param [in] sql
         *      an SQL query returning the target values as the first column;
         *      <em>the results must be sequenced in ascending order of the
         *      first column</em>
         * \param [in] args
         *      zero or more arguments to be bound to any parameters in \c sql
         *
         * \return reference to \c *this
         */
        this_t &symmetric_difference(const this_t &other);
        this_t &symmetric_difference(Statement &stmt, int col_no = 0);
        this_t &symmetric_difference(std::initializer_list<ID> ids);

        template <typename SrcIter> this_t &
                symmetric_difference(SrcIter first, SrcIter last);

        template <typename ... Args> this_t &
             symmetric_difference_sql(const u8string_view &sql, Args &&...args);
        ///@}

        /**
         * \brief remove all elements
         *
         * \note
         *      Any iterators referencing elements of \c this prior to calling
         *      \c clear() will be invalidated.
         *
         * \return reference to \c *this
         */
        this_t &clear();

        /**
         * \brief exchange state with another IDSet object
         *
         * \c swap() exchanges both the elements and database connection
         * attachment states of \c this and \c other. Both \c IDSet objects'
         * temporary table names remain the same and refer to the original
         * objects; they are not swapped. Thus, if both \c IDSet objects were
         * attached to the same database connection then it is safe to use
         * statements involving them that were prepared before the swap. If
         * the swapped objects were attached to different connections then
         * any previously-prepared statements involving the swapped objects
         * must be prepared again before re-executing them.
         *
         * \param [in,out] other
         *      the object to swap with \c *this
         *
         * \return reference to \c *this
         */
        this_t &swap(this_t &other);

        ///@{
        /**
         *
         * \brief obtain a forward iterator to the smallest element
         * \return forward iterator referencing the smallest element or
         *      \c end() if the \c IDSet is empty
         */
        iterator begin()                       { return cbegin(); }
        const_iterator begin() const           { return cbegin(); }
        const_iterator cbegin() const;
        ///@}

        ///@{
        /**
         * \brief obtain a forward sentinel iterator
         * \return forward iterator referencing the point beyond the largest
         *      element
         */
        iterator end()                         { return cend(); }
        const_iterator end() const             { return cend(); }
        const_iterator cend() const;
        ///@}

        ///@{
        /**
         * \brief obtain a reverse iterator to the largest element
         * \return reverse iterator referencing the smallest element or
         *      \c rend() if the \c IDSet is empty
         */
        reverse_iterator rbegin()              { return crbegin(); }
        const_reverse_iterator rbegin() const  { return crbegin(); }
        const_reverse_iterator crbegin() const;
        ///@}

        ///@{
        /**
         * \brief obtain a reverse sentinel iterator
         * \return reverse iterator referencing the point beyond the smallest
         *      element
         */
        reverse_iterator rend()                { return crend(); }
        const_reverse_iterator rend() const    { return crend(); }
        const_reverse_iterator crend() const;
        ///@}

        /**
         * \brief check for empty set
         * \return \c true if the \c IDSet is empty or \c false otherwise
         */
        bool empty() const;

        /**
         * \brief query the number of elements in the set
         * \return number of elements stored
         */
        size_type size() const;

        /**
         * \brief query the maximum possible size of an \c IDSet
         * \return maximum possible size
         */
        size_type max_size() const;

        /**
         * \brief get the amount of space presently allocated
         * \return number of elements which can be stored without requiring
         *      extra memory
         */
        size_type capacity() const;

        /**
         * \brief retrieve the name of the temporary table
         * \return temporary table name
         */
        std::string sql_name() const;

        /**
         * \brief query the number of occurrences of \c id
         * \return \c 0 if \c id is present, \c 1 otherwise
         */
        size_type count(ID id) const;

        ///@{
        /**
         * \brief search for an element matching the value given
         *
         * \param [in] id
         *      the value to search for
         *
         * \return
         *      iterator referencing corresponding element if found or
         *      \c end() otherwise
         */
        const_iterator find(ID id) const;
        iterator find(ID id)
                { return static_cast<const this_t *>(this)->find(id); }
        ///@}

        ///@{
        /**
         * \brief search for the first element not less than the value given
         *
         * \param [in] id
         *      the bounding value
         *
         * \return
         *      iterator referencing the first element not less than \c id or
         *      \c end() if no such element exists
         */
        const_iterator lower_bound(ID id) const;
        iterator lower_bound(ID id)
                { return static_cast<const this_t *>(this)->lower_bound(id); }
        ///@}

        ///@{
        /**
         * \brief search for the first element greater than the value given
         *
         * \param [in] id
         *      the bounding value
         *
         * \return iterator referencing the first element greater than \c id
         *      or \c end() if no such element exists
         */
        const_iterator upper_bound(ID id) const;
        iterator upper_bound(ID id)
                { return static_cast<const this_t *>(this)->upper_bound(id); }
        ///@}

        ///@{
        /**
         * \brief search for a range of elements matching the given value
         *
         * \param [in] id
         *      the value to search for
         *
         * \return
         *      a pair of iterators defining the matching range (first
         *      iterator referencing the matching element, second referencing
         *      the point immediately after the first) or a pair of equal
         *      iterators if no matching element exists
         */
        std::pair<const_iterator, const_iterator> equal_range(ID id) const;
        std::pair<iterator, iterator> equal_range(ID id)
                { return static_cast<const this_t *>(this)->equal_range(id); }
        ///@}

        /**
         * \brief retrieve the <i>i</i>'th smallest element
         *
         * \param [in] i
         *      the zero-based index of the element to be retrieved
         * \return
         *      the value at index \c i
         * \note
         *      \c i is not checked for validity; the results of invoking
         *      <code>operator[]</code> with \c i equal to or greater than
         *      \c size() are undefined.
         */
        ID operator[](size_t i) const;

        /**
         * \brief request allocation of memory ahead-of-time
         *
         * \c reserve() has no effect if \c n is the same as or less than
         * \c size() or \c capacity().
         *
         * \note
         *      Any iterators referencing elements of \c this prior to calling
         *      \c reserve() must be considered invalid upon return.
         *
         * \param [in] n
         *      number of elements to allocate space for
         *
         * \return reference to \c *this
         */
        this_t &reserve(size_t n);

        /**
         * \brief request deallocation of unused memory
         *
         * \note
         *      Any iterators referencing elements of \c this prior to calling
         *      \c shrink_to_fit() must be considered invalid upon return.
         *
         * \return reference to \c *this
         */
        this_t &shrink_to_fit();

        ///@{
        /**
         * \brief compare the contents of two \c IDSet objects
         *
         * Neither \c a nor \c b are required to be attached to a database.
         *
         * \param [in] a
         *      first object for comparison
         * \param [in] b
         *      second object for comparison
         *
         * \return
         *      <code>operator==</code> returns \c true if \c a and \c b
         *      both contain the same values or \c false otherwise
         * \return
         *      <code>operator!=</code> returns \c true if \c a and \c b
         *      contain different values or \c false otherwise
         * \return
         *      <code>operator\<</code> returns \c true if the value(s) in
         *      \c a compare lexicographically less than the value(s) in \c b
         *      or \c false otherwise
         * \return
         *      <code>operator\<=</code> returns \c true if the \c a and \c b
         *      both contain the same values or the value(s) in \c a compare
         *      lexicographically less than the value(s) in \c b or \c false
         *      otherwise
         * \return
         *      <code>operator\></code> returns \c true if the value(s) in
         *      \c a compare lexicographically greater than the value(s) in
         *      \c b or \c false otherwise
         * \return
         *      <code>operator\>=</code> returns \c true if the \c a and \c b
         *      both contain the same values or the value(s) in \c a compare
         *      lexicographically greater than the value(s) in \c b or
         *      \c false otherwise
         */
        friend bool operator==(const this_t &a, const this_t &b);
        friend bool operator!=(const this_t &a, const this_t &b);
        friend bool operator<(const this_t &a, const this_t &b);
        friend bool operator<=(const this_t &a, const this_t &b);
        friend bool operator>(const this_t &a, const this_t &b);
        friend bool operator>=(const this_t &a, const this_t &b);
        ///@}

        class SQLInterface;  // opaque internal type
        friend SQLInterface;

private:
        friend class IDSetTests;
        struct Body;

        void checkAttached(const char *context) const;

        Body *body_;
};

//--------------------------------------

template <typename SrcIter> inline auto
IDSet::insert(
        SrcIter first,
        SrcIter last
) -> size_type
{
        size_type n = 0;

        for (; first != last; ++first) {
                if (insert(*first).second) {
                        ++n;
                }
        }

        return n;
}

//--------------------------------------

template <typename ... Args> inline auto
IDSet::insert_sql(
        const u8string_view     &sql,
        Args                &&...args
) -> size_type
{
        checkAttached("IDSet::insert_sql()");
        Statement stmt(*db(), sql);
        return insert(stmt.bindAll(std::forward<Args>(args)...));
}

//--------------------------------------

template <typename ... Args> inline auto
IDSet::erase_sql(
        const u8string_view     &sql,
        Args                &&...args
) -> size_type
{
        checkAttached("IDSet::erase_sql()");
        Statement stmt(*db(), sql);
        return erase(stmt.bindAll(std::forward<Args>(args)...));
}

//--------------------------------------

template <typename SrcIter> inline auto
IDSet::intersect(
        SrcIter first,
        SrcIter last
) -> size_type
{
        IDSet tmp;
        tmp.insert(first, last);
        return intersect(tmp);
}

//--------------------------------------

template <typename ... Args> inline auto
IDSet::intersect_sql(
        const u8string_view     &sql,
        Args                &&...args
) -> size_type
{
        if (empty()) {
                return 0;
        }
        checkAttached("IDSet::intersect_sql()");
        Statement stmt(*db(), sql);
        return intersect(stmt.bindAll(std::forward<Args>(args)...));
}

//--------------------------------------

template <typename SrcIter> inline auto
IDSet::symmetric_difference(
        SrcIter first,
        SrcIter last
) -> this_t &
{
        this_t tmp;
        tmp.insert(first, last);
        return symmetric_difference(tmp);
}

//--------------------------------------

template <typename ... Args> inline auto
IDSet::symmetric_difference_sql(
        const u8string_view     &sql,
        Args                &&...args
) -> this_t &
{
        checkAttached("IDSet::symmetric_difference_sql()");
        Statement stmt(*db(), sql);
        return symmetric_difference(stmt.bindAll(std::forward<Args>(args)...));
}


} // namespace sql


//--------------------------------------
/*
 * support wr::print() formatting of IDSet objects
 */
namespace fmt {


struct Arg;
struct Params;
template <typename> struct TypeHandler;

template <> struct WRSQL_API TypeHandler<wr::sql::IDSet>
{
        static void set(Arg &arg, const wr::sql::IDSet &val);
        static bool format(const Params &parms);
};


} // namespace fmt
} // namespace wr

//--------------------------------------

namespace std {


template <> inline void
swap<wr::sql::IDSet>(
        wr::sql::IDSet &a,
        wr::sql::IDSet &b
)
{
        a.swap(b);
}


} // namespace std


#endif // !WRSQL_ID_SET_H
